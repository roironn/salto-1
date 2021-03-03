/*
*                      Copyright 2021 Salto Labs Ltd.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
import _ from 'lodash'
import path from 'path'
import { Element, SaltoError, SaltoElementError, ElemID, InstanceElement, DetailedChange, Change,
  Value,
  toChange,
  isRemovalChange,
  getChangeElement } from '@salto-io/adapter-api'
import { logger } from '@salto-io/logging'
import { collections, promises } from '@salto-io/lowerdash'
import { applyDetailedChanges } from '@salto-io/adapter-utils'
import { validateElements } from '../validator'
import { SourceRange, ParseError, SourceMap } from '../parser'
import { ConfigSource } from './config_source'
import { State } from './state'
import { NaclFilesSource, NaclFile, RoutingMode } from './nacl_files/nacl_files_source'
import { ParsedNaclFile } from './nacl_files/parsed_nacl_file'
import { multiEnvSource, getSourceNameForFilename, MultiEnvSource } from './nacl_files/multi_env/multi_env_source'
import { ElementSelector } from './element_selector'
import { Errors, ServiceDuplicationError, EnvDuplicationError, UnknownEnvError, DeleteCurrentEnvError } from './errors'
import { EnvConfig } from './config/workspace_config_types'
import { handleHiddenChanges, getElementHiddenParts, isHidden } from './hidden_values'
import { WorkspaceConfigSource } from './workspace_config_source'
import { MergeError, mergeElements } from '../merger'
import { RemoteElementSource, ElementsSource, mapReadOnlyElementsSource } from './elements_source'
import { buildNewMergedElementsAndErrors, getBuildMergeData } from './nacl_files/elements_cache'
import { RemoteMap, RemoteMapCreator } from './remote_map'
import { serialize, deserializeMergeErrors, deserializeSingleElement } from '../serializer/elements'

const log = logger(module)

const { makeArray } = collections.array
const { awu } = collections.asynciterable

export const ADAPTERS_CONFIGS_PATH = 'adapters'
export const COMMON_ENV_PREFIX = ''
const DEFAULT_STALE_STATE_THRESHOLD_MINUTES = 60 * 24 * 7 // 7 days

export type WorkspaceError<T extends SaltoError> = Readonly<T & {
  sourceFragments: SourceFragment[]
}>

export type SourceFragment = {
  sourceRange: SourceRange
  fragment: string
  subRange?: SourceRange
}

type RecencyStatus = 'Old' | 'Nonexistent' | 'Valid'
export type StateRecency = {
  serviceName: string
  status: RecencyStatus
  date: Date | undefined
}

export type WorkspaceComponents = {
  nacl: boolean
  state: boolean
  cache: boolean
  staticResources: boolean
  credentials: boolean
  serviceConfig: boolean
}

export type Workspace = {
  uid: string
  name: string

  elements: (includeHidden?: boolean, env?: string) => Promise<ElementsSource>
  state: (envName?: string) => State
  envs: () => ReadonlyArray<string>
  currentEnv: () => string
  services: () => string[]
  servicesCredentials: (names?: ReadonlyArray<string>) =>
    Promise<Readonly<Record<string, InstanceElement>>>
  servicesConfig: (names?: ReadonlyArray<string>) =>
    Promise<Readonly<Record<string, InstanceElement>>>

  isEmpty(naclFilesOnly?: boolean): Promise<boolean>
  hasElementsInServices(serviceNames: string[]): Promise<boolean>
  hasElementsInEnv(envName: string): Promise<boolean>
  envOfFile(filename: string): string
  getSourceFragment(sourceRange: SourceRange): Promise<SourceFragment>
  hasErrors(): Promise<boolean>
  errors(validate?: boolean): Promise<Readonly<Errors>>
  transformToWorkspaceError<T extends SaltoElementError>(saltoElemErr: T):
    Promise<Readonly<WorkspaceError<T>>>
  transformError: (error: SaltoError) => Promise<WorkspaceError<SaltoError>>
  updateNaclFiles: (changes: DetailedChange[], mode?: RoutingMode) => Promise<void>
  listNaclFiles: () => Promise<string[]>
  getTotalSize: () => Promise<number>
  getNaclFile: (filename: string) => Promise<NaclFile | undefined>
  setNaclFiles: (...naclFiles: NaclFile[]) => Promise<Change[]>
  removeNaclFiles: (...names: string[]) => Promise<Change[]>
  getSourceMap: (filename: string) => Promise<SourceMap>
  getSourceRanges: (elemID: ElemID) => Promise<SourceRange[]>
  getElementReferencedFiles: (id: ElemID) => Promise<string[]>
  getElementNaclFiles: (id: ElemID) => Promise<string[]>
  getElementIdsBySelectors: (selectors: ElementSelector[],
    commonOnly?: boolean) => Promise<AsyncIterable<ElemID>>
  getParsedNaclFile: (filename: string) => Promise<ParsedNaclFile | undefined>
  flush: () => Promise<void>
  clone: () => Promise<Workspace>
  clear: (args: Omit<WorkspaceComponents, 'serviceConfig'>) => Promise<void>

  addService: (service: string) => Promise<void>
  addEnvironment: (env: string) => Promise<void>
  deleteEnvironment: (env: string) => Promise<void>
  renameEnvironment: (envName: string, newEnvName: string, newSourceName? : string) => Promise<void>
  setCurrentEnv: (env: string, persist?: boolean) => Promise<void>
  updateServiceCredentials: (service: string, creds: Readonly<InstanceElement>) => Promise<void>
  updateServiceConfig: (service: string, newConfig: Readonly<InstanceElement>) => Promise<void>

  getStateRecency(services: string): Promise<StateRecency>
  promote(ids: ElemID[]): Promise<void>
  demote(ids: ElemID[]): Promise<void>
  demoteAll(): Promise<void>
  copyTo(ids: ElemID[], targetEnvs?: string[]): Promise<void>
  getValue(id: ElemID): Promise<Value | undefined>
}

// common source has no state
export type EnvironmentSource = { naclFiles: NaclFilesSource; state?: State }
export type EnvironmentsSources = {
  commonSourceName: string
  sources: Record<string, EnvironmentSource>
}

type WorkspaceState = {
  merged: ElementsSource
  errors: RemoteMap<MergeError[]>
}

export const loadWorkspace = async (
  config: WorkspaceConfigSource,
  credentials: ConfigSource,
  elementsSources: EnvironmentsSources,
  remoteMapCreator: RemoteMapCreator,
  ignoreFileChanges = false,
): Promise<Workspace> => {
  const workspaceConfig = await config.getWorkspaceConfig()
  log.debug('Loading workspace with id: %s', workspaceConfig.uid)

  if (_.isEmpty(workspaceConfig.envs)) {
    throw new Error('Workspace with no environments is illegal')
  }
  const envs = (): ReadonlyArray<string> => workspaceConfig.envs.map(e => e.name)
  const currentEnv = (): string => workspaceConfig.currentEnv ?? workspaceConfig.envs[0].name
  const getRemoteMapNamespace = (
    namespace: string, env?: string
  ): string => `workspace-${env || currentEnv()}-${namespace}`
  const currentEnvConf = (): EnvConfig =>
    makeArray(workspaceConfig.envs).find(e => e.name === currentEnv()) as EnvConfig
  const currentEnvsConf = (): EnvConfig[] =>
    workspaceConfig.envs
  const services = (): string[] => makeArray(currentEnvConf().services)
  const state = (envName?: string): State => (
    elementsSources.sources[envName ?? currentEnv()].state as State
  )
  let naclFilesSource = multiEnvSource(_.mapValues(elementsSources.sources, e => e.naclFiles),
    currentEnv(), elementsSources.commonSourceName, remoteMapCreator)
  let workspaceState: Promise<WorkspaceState> | undefined

  const buildWorkspaceState = async ({ workspaceChanges = [], env, stateChanges = [] }: {
    workspaceChanges?: Change<Element>[]
    env?: string
    stateChanges?: Change<Element>[]
  }): Promise<WorkspaceState> => {
    const actualEnv = env ?? currentEnv()
    const stateToBuild = workspaceState !== undefined && actualEnv === currentEnv()
      ? await workspaceState : {
        merged: new RemoteElementSource(
          await remoteMapCreator<Element>({
            namespace: getRemoteMapNamespace('merged', actualEnv),
            serialize: element => serialize([element]),
            // TODO: we might need to pass static file reviver to the deserialization func
            deserialize: deserializeSingleElement,
          })
        ),
        errors: await remoteMapCreator<MergeError[]>({
          namespace: getRemoteMapNamespace('errors', actualEnv),
          serialize: mergeErrors => serialize(mergeErrors),
          deserialize: async data => deserializeMergeErrors(data),
        }),
      }

    const initHiddenElementsChanges = await awu(await stateToBuild.merged.getAll()).isEmpty()
      && _.isEmpty(stateChanges)
      ? await awu(await state().getAll()).filter(element => isHidden(element, state()))
        .map(elem => toChange({ after: elem })).toArray()
      : []

    const stateRemovedElementChanges = workspaceChanges
      .filter(change => isRemovalChange(change) && getChangeElement(change).elemID.isTopLevel())

    const mergeData = await getBuildMergeData({
      src1Changes: workspaceChanges,
      src1: stateToBuild.merged,
      src2Changes: stateChanges
        .concat(initHiddenElementsChanges)
        .concat(stateRemovedElementChanges),
      src2: mapReadOnlyElementsSource(
        state(),
        async element => getElementHiddenParts(
          element,
          state(),
          await stateToBuild.merged.get(element.elemID)
        )
      ),
    })

    await buildNewMergedElementsAndErrors({
      currentElements: stateToBuild.merged,
      currentErrors: stateToBuild.errors,
      mergeFunc: elements => mergeElements(elements),
      ...mergeData,
    })

    return stateToBuild
  }


  const getWorkspaceState = async (): Promise<WorkspaceState> => {
    if (_.isUndefined(workspaceState)) {
      const workspaceChanges = await naclFilesSource.load(ignoreFileChanges)
      workspaceState = buildWorkspaceState({ workspaceChanges })
    }
    return workspaceState
  }

  const getLoadedNaclFilesSource = async (): Promise<MultiEnvSource> => {
    // We load the nacl file source, and make sure the state of the WS is also
    // updated. (Without this - the load changes will be lost)
    await getWorkspaceState()
    return naclFilesSource
  }

  const elements = async (env?: string): Promise<WorkspaceState> => {
    if (env && env !== currentEnv()) {
      const changes = await naclFilesSource.load(undefined, undefined, env)
      return buildWorkspaceState({ env, workspaceChanges: changes })
    }
    return getWorkspaceState()
  }

  const getStateChanges = async (
    hiddenChanges: DetailedChange[],
  ): Promise<Change[]> => {
    const changesByID = _.groupBy(
      hiddenChanges,
      change => change.id.createTopLevelParentID().parent.getFullName()
    )

    return awu(Object.values(changesByID)).flatMap(async changes => {
      const refID = changes[0].id
      if (refID.isTopLevel()) {
        return changes as Change[]
      }
      const before = await state().get(refID.createTopLevelParentID().parent)
      const clonedBefore = before.clone()
      applyDetailedChanges(clonedBefore, changes)
      const after = await getElementHiddenParts(
        clonedBefore,
        state(),
        before
      )
      return after !== undefined ? [toChange({ before, after })] : []
    }).toArray()
  }

  const updateNaclFiles = async (
    changes: DetailedChange[],
    mode?: RoutingMode
  ): Promise<void> => {
    const { visible: visibleChanges, hidden: hiddenChanges } = await handleHiddenChanges(
      changes,
      state(),
      (await getLoadedNaclFilesSource()).getAll,
    )
    const workspaceChanges = await (await getLoadedNaclFilesSource())
      .updateNaclFiles(visibleChanges, mode)
    const stateChanges = await getStateChanges(hiddenChanges)
    workspaceState = buildWorkspaceState({ workspaceChanges, stateChanges })
  }


  const setNaclFiles = async (...naclFiles: NaclFile[]): Promise<Change[]> => {
    const elementChanges = await (await getLoadedNaclFilesSource()).setNaclFiles(...naclFiles)
    workspaceState = buildWorkspaceState({ workspaceChanges: elementChanges })
    return elementChanges
  }

  const removeNaclFiles = async (...names: string[]): Promise<Change[]> => {
    const elementChanges = await (await getLoadedNaclFilesSource()).removeNaclFiles(...names)
    workspaceState = buildWorkspaceState({ workspaceChanges: elementChanges })
    return elementChanges
  }

  const getSourceFragment = async (
    sourceRange: SourceRange, subRange?: SourceRange): Promise<SourceFragment> => {
    const naclFile = await (await getLoadedNaclFilesSource()).getNaclFile(sourceRange.filename)
    log.debug(`error context: start=${sourceRange.start.byte}, end=${sourceRange.end.byte}`)
    const fragment = naclFile
      ? naclFile.buffer.substring(sourceRange.start.byte, sourceRange.end.byte)
      : ''
    if (!naclFile) {
      log.warn('failed to resolve source fragment for %o', sourceRange)
    }
    return {
      sourceRange,
      fragment,
      subRange,
    }
  }
  const transformParseError = async (error: ParseError): Promise<WorkspaceError<SaltoError>> => ({
    ...error,
    sourceFragments: [await getSourceFragment(error.context, error.subject)],
  })
  const transformToWorkspaceError = async <T extends SaltoElementError>(saltoElemErr: T):
    Promise<Readonly<WorkspaceError<T>>> => {
    const sourceRanges = await (await getLoadedNaclFilesSource())
      .getSourceRanges(saltoElemErr.elemID)
    const sourceFragments = await awu(sourceRanges)
      .map(range => getSourceFragment(range))
      .toArray()

    return {
      ...saltoElemErr,
      message: saltoElemErr.message,
      sourceFragments,
    }
  }
  const transformError = async (error: SaltoError): Promise<WorkspaceError<SaltoError>> => {
    const isParseError = (err: SaltoError): err is ParseError =>
      _.has(err, 'subject')
    const isElementError = (err: SaltoError): err is SaltoElementError =>
      _.get(err, 'elemID') instanceof ElemID
    if (isParseError(error)) {
      return transformParseError(error)
    }
    if (isElementError(error)) {
      return transformToWorkspaceError(error)
    }
    return { ...error, sourceFragments: [] }
  }

  const errors = async (validate = true): Promise<Errors> => {
    const resolvedElements = await elements()
    const errorsFromSource = await (await getLoadedNaclFilesSource()).getErrors()

    const validationErrors = validate
      ? await validateElements(
        await awu(await resolvedElements.merged.getAll()).toArray(),
        resolvedElements.merged
      ) : []

    _(validationErrors)
      .groupBy(error => error.constructor.name)
      .entries()
      .forEach(([errorType, errorsGroup]) => {
        log.error(`Invalid elements, error type: ${errorType}, element IDs: ${errorsGroup.map(e => e.elemID.getFullName()).join(', ')}`)
      })

    return new Errors({
      ...errorsFromSource,
      merge: [
        ...errorsFromSource.merge,
        ...(await awu(resolvedElements.errors.values()).flat().toArray()),
      ],
      validation: validationErrors,
    })
  }

  const pickServices = (names?: ReadonlyArray<string>): ReadonlyArray<string> =>
    (_.isUndefined(names) ? services() : services().filter(s => names.includes(s)))
  const credsPath = (service: string): string => path.join(currentEnv(), service)
  return {
    uid: workspaceConfig.uid,
    name: workspaceConfig.name,
    elements: async (includeHidden = true, env) => {
      if (includeHidden) {
        return (await elements(env)).merged
      }
      return ((await getLoadedNaclFilesSource())).getElementsSource(env)
    },
    state,
    envs,
    currentEnv,
    services,
    errors,
    hasErrors: async () => (await errors()).hasErrors(),
    servicesCredentials: async (names?: ReadonlyArray<string>) => _.fromPairs(await Promise.all(
      pickServices(names).map(async service => [service, await credentials.get(credsPath(service))])
    )),
    servicesConfig: async (names?: ReadonlyArray<string>) => _.fromPairs(await Promise.all(
      pickServices(names).map(
        async service => [
          service,
          (await config.getAdapter(service)),
        ]
      )
    )),
    isEmpty: async (naclFilesOnly = false): Promise<boolean> => {
      const isNaclFilesSourceEmpty = !naclFilesSource
        || await (await getLoadedNaclFilesSource()).isEmpty()
      return isNaclFilesSourceEmpty && (naclFilesOnly || state().isEmpty())
    },
    hasElementsInServices: async (serviceNames: string[]): Promise<boolean> => (
      await (awu(await (await getLoadedNaclFilesSource()).list()).find(
        elemId => serviceNames.includes(elemId.adapter)
      )) !== undefined
    ),
    hasElementsInEnv: async envName => {
      const envSource = elementsSources.sources[envName]
      if (envSource === undefined) {
        return false
      }
      return !(await envSource.naclFiles.isEmpty())
    },
    envOfFile: filename => getSourceNameForFilename(
      filename, envs() as string[], elementsSources.commonSourceName
    ),
    // Returning the functions from the nacl file source directly (eg: promote: src.promote)
    // may seem better, but the setCurrentEnv method replaced the naclFileSource.
    // Passing direct pointer for these functions would have resulted in pointers to a nullified
    // source so we need to wrap all of the function calls to make sure we are forwarding the method
    // invocations to the proper source.
    setNaclFiles,
    updateNaclFiles,
    removeNaclFiles,
    getSourceMap: async (filename: string) => (
      (await getLoadedNaclFilesSource()).getSourceMap(filename)
    ),
    getSourceRanges: async (elemID: ElemID) => (
      (await getLoadedNaclFilesSource()).getSourceRanges(elemID)
    ),
    listNaclFiles: async () => (
      (await getLoadedNaclFilesSource()).listNaclFiles()
    ),
    getElementIdsBySelectors: async (selectors: ElementSelector[],
      commonOnly = false) => (
      (await getLoadedNaclFilesSource()).getElementIdsBySelectors(selectors, commonOnly)
    ),
    getElementReferencedFiles: async id => (
      (await getLoadedNaclFilesSource()).getElementReferencedFiles(id)
    ),
    getElementNaclFiles: async id => (
      (await getLoadedNaclFilesSource()).getElementNaclFiles(id)
    ),
    getTotalSize: async () => (
      (await getLoadedNaclFilesSource()).getTotalSize()
    ),
    getNaclFile: async (filename: string) => (
      (await getLoadedNaclFilesSource()).getNaclFile(filename)
    ),
    getParsedNaclFile: async (filename: string) => (
      (await getLoadedNaclFilesSource()).getParsedNaclFile(filename)
    ),
    promote: async (ids: ElemID[]) => (
      (await getLoadedNaclFilesSource()).promote(ids)
    ),
    demote: async (ids: ElemID[]) => (await getLoadedNaclFilesSource()).demote(ids),
    demoteAll: async () => (await getLoadedNaclFilesSource()).demoteAll(),
    copyTo: async (ids: ElemID[],
      targetEnvs: string[]) => (await getLoadedNaclFilesSource()).copyTo(ids, targetEnvs),
    transformToWorkspaceError,
    transformError,
    getSourceFragment,
    flush: async (): Promise<void> => {
      const currentWSState = await getWorkspaceState()
      await state().flush()
      await (await getLoadedNaclFilesSource()).flush()
      await currentWSState.merged.flush()
      await currentWSState.errors.flush()
    },
    clone: (): Promise<Workspace> => {
      const sources = _.mapValues(elementsSources.sources, source =>
        ({ naclFiles: source.naclFiles.clone(), state: source.state }))
      const envSources = { commonSourceName: elementsSources.commonSourceName, sources }
      return loadWorkspace(config, credentials, envSources, remoteMapCreator)
    },
    clear: async (args: Omit<WorkspaceComponents, 'serviceConfig'>) => {
      if (args.cache || args.nacl || args.staticResources) {
        if (args.staticResources && !(args.state && args.cache && args.nacl)) {
          throw new Error('Cannot clear static resources without clearing the state, cache and nacls')
        }
        await (await getLoadedNaclFilesSource()).clear(args)
      }
      if (args.state) {
        await promises.array.series(envs().map(e => (() => state(e).clear())))
      }
      if (args.credentials) {
        await promises.array.series(envs().map(e => (() => credentials.delete(e))))
      }
      workspaceState = buildWorkspaceState({})
    },
    addService: async (service: string): Promise<void> => {
      const currentServices = services() || []
      if (currentServices.includes(service)) {
        throw new ServiceDuplicationError(service)
      }
      currentEnvConf().services = [...currentServices, service]
      await config.setWorkspaceConfig(workspaceConfig)
    },
    updateServiceCredentials:
      async (service: string, servicesCredentials: Readonly<InstanceElement>): Promise<void> =>
        credentials.set(credsPath(service), servicesCredentials),
    updateServiceConfig:
      async (service: string, newConfig: Readonly<InstanceElement>): Promise<void> => {
        await config.setAdapter(service, newConfig)
      },
    addEnvironment: async (env: string): Promise<void> => {
      if (workspaceConfig.envs.map(e => e.name).includes(env)) {
        throw new EnvDuplicationError(env)
      }
      // Need to make sure everything is loaded before we add the new env.
      await getWorkspaceState()
      workspaceConfig.envs = [...workspaceConfig.envs, { name: env }]
      await config.setWorkspaceConfig(workspaceConfig)
    },
    deleteEnvironment: async (env: string): Promise<void> => {
      if (!(workspaceConfig.envs.map(e => e.name).includes(env))) {
        throw new UnknownEnvError(env)
      }
      if (env === currentEnv()) {
        throw new DeleteCurrentEnvError(env)
      }
      workspaceConfig.envs = workspaceConfig.envs.filter(e => e.name !== env)
      await config.setWorkspaceConfig(workspaceConfig)

      // We assume here that all the credentials files sit under the credentials' env directory
      await credentials.delete(env)

      const environmentSource = elementsSources.sources[env]
      if (environmentSource) {
        await environmentSource.naclFiles.clear()
        await environmentSource.state?.clear()
      }
      delete elementsSources.sources[env]
      naclFilesSource = multiEnvSource(
        _.mapValues(elementsSources.sources, e => e.naclFiles),
        currentEnv(),
        elementsSources.commonSourceName,
        remoteMapCreator,
      )
    },
    renameEnvironment: async (envName: string, newEnvName: string, newEnvNaclPath? : string) => {
      const envConfig = envs().find(e => e === envName)
      if (_.isUndefined(envConfig)) {
        throw new UnknownEnvError(envName)
      }

      if (!_.isUndefined(envs().find(e => e === newEnvName))) {
        throw new EnvDuplicationError(newEnvName)
      }

      currentEnvsConf()
        .filter(e => e.name === envName)
        .forEach(e => {
          e.name = newEnvName
        })
      if (envName === workspaceConfig.currentEnv) {
        workspaceConfig.currentEnv = newEnvName
      }
      await config.setWorkspaceConfig(workspaceConfig)
      await credentials.rename(envName, newEnvName)
      const environmentSource = elementsSources.sources[envName]
      if (environmentSource) {
        await environmentSource.naclFiles.rename(newEnvNaclPath || newEnvName)
        await environmentSource.state?.rename(newEnvName)
      }
      elementsSources.sources[newEnvName] = environmentSource
      delete elementsSources.sources[envName]
      naclFilesSource = multiEnvSource(
        _.mapValues(elementsSources.sources, e => e.naclFiles),
        currentEnv(),
        elementsSources.commonSourceName,
        remoteMapCreator,
      )
    },
    setCurrentEnv: async (env: string, persist = true): Promise<void> => {
      if (!envs().includes(env)) {
        throw new UnknownEnvError(env)
      }
      workspaceConfig.currentEnv = env
      if (persist) {
        await config.setWorkspaceConfig(workspaceConfig)
      }
      // naclFilesSource.setCurrentEnv(env)
      // const changes = await naclFilesSource.load()
      naclFilesSource = multiEnvSource(
        _.mapValues(elementsSources.sources, e => e.naclFiles),
        currentEnv(),
        elementsSources.commonSourceName,
        remoteMapCreator,
      )
      workspaceState = undefined // buildWorkspaceState({ workspaceChanges: changes })
    },

    getStateRecency: async (serviceName: string): Promise<StateRecency> => {
      const staleStateThresholdMs = (workspaceConfig.staleStateThresholdMinutes
        || DEFAULT_STALE_STATE_THRESHOLD_MINUTES) * 60 * 1000
      const date = (await state().getServicesUpdateDates())[serviceName]
      const status = (() => {
        if (date === undefined) {
          return 'Nonexistent'
        }
        if (Date.now() - date.getTime() >= staleStateThresholdMs) {
          return 'Old'
        }
        return 'Valid'
      })()
      return { serviceName, status, date }
    },
    getValue: async (id: ElemID, env?: string): Promise<Value | undefined> => (
      (await elements(env)).merged.get(id)
    ),
  }
}

export const initWorkspace = async (
  name: string,
  uid: string,
  defaultEnvName: string,
  config: WorkspaceConfigSource,
  credentials: ConfigSource,
  envs: EnvironmentsSources,
  remoteMapCreator: RemoteMapCreator,
): Promise<Workspace> => {
  log.debug('Initializing workspace with id: %s', uid)
  await config.setWorkspaceConfig({
    uid,
    name,
    envs: [{ name: defaultEnvName }],
    currentEnv: defaultEnvName,
  })
  return loadWorkspace(config, credentials, envs, remoteMapCreator)
}
