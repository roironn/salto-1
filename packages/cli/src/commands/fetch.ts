/*
*                      Copyright 2020 Salto Labs Ltd.
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
import wu from 'wu'
import { getChangeElement, isInstanceElement } from '@salto-io/adapter-api'
import {
  fetch as apiFetch,
  Workspace,
  FetchFunc,
  FetchChange,
  FetchProgressEvents,
  StepEmitter,
  Telemetry,
  PlanItem,
} from '@salto-io/core'
import { promises } from '@salto-io/lowerdash'
import { EventEmitter } from 'pietile-eventemitter'
import { logger } from '@salto-io/logging'
import { EOL } from 'os'
import { environmentFilter } from '../filters/env'
import { createCommandBuilder } from '../command_builder'
import {
  ParsedCliInput, CliCommand, CliOutput,
  CliExitCode, SpinnerCreator, CliTelemetry,
} from '../types'
import {
  formatChangesSummary, formatMergeErrors, formatFatalFetchError, formatStepStart,
  formatStepCompleted, formatStepFailed, formatFetchHeader, formatFetchFinish,
  formatApproveIsolatedModePrompt,
  formatStateChanges,
} from '../formatter'
import { getApprovedChanges as cliGetApprovedChanges,
  shouldUpdateConfig as cliShouldUpdateConfig,
  cliApproveIsolatedMode } from '../callbacks'
import { updateWorkspace, loadWorkspace, getWorkspaceTelemetryTags, updateStateOnly } from '../workspace/workspace'
import Prompts from '../prompts'
import { servicesFilter, ServicesArgs } from '../filters/services'
import { getCliTelemetry } from '../telemetry'
import { EnvironmentArgs } from './env'

const log = logger(module)
const { series } = promises.array

type ApproveChangesFunc = (
  changes: ReadonlyArray<FetchChange>,
  interactive: boolean
) => Promise<ReadonlyArray<FetchChange>>

type ShouldUpdateConfigFunc = (
  { stdout }: CliOutput,
  introMessage: string,
  change: PlanItem
) => Promise<boolean>

type ApproveIsolatedModeFunc = (
  newServices: string[],
  oldServices: string[],
  inputIsolated: boolean
) => Promise<boolean>

export type FetchCommandArgs = {
  workspace: Workspace
  force: boolean
  interactive: boolean
  inputIsolated: boolean
  cliTelemetry: CliTelemetry
  output: CliOutput
  fetch: FetchFunc
  getApprovedChanges: ApproveChangesFunc
  shouldUpdateConfig: ShouldUpdateConfigFunc
  approveIsolatedMode: ApproveIsolatedModeFunc
  shouldCalcTotalSize: boolean
  stateOnly: boolean
  inputServices?: string[]
}

const shouldRecommendIsolatedModeForNewServices = (
  servicesNewOnlyInCurrentEnv: string[],
  existingOrNewToAllEnvsServices: string[],
  envNames: readonly string[],
  isolatedOveride?: boolean
): boolean => {
  if (_.isEmpty(servicesNewOnlyInCurrentEnv)) return false
  if (envNames.length <= 1) return false
  if (!_.isEmpty(existingOrNewToAllEnvsServices) && isolatedOveride) return true
  if (_.isEmpty(existingOrNewToAllEnvsServices) && !isolatedOveride) return true
  return false
}

const getRelevantServicesAndIsolatedMode = async (
  inputServices: string[],
  workspace: Workspace,
  inputIsolated: boolean,
  force: boolean,
  approveIsolatedMode: ApproveIsolatedModeFunc,
  outputLine: (text: string) => void
): Promise<{services: string[]; isolated: boolean}> => {
  const envNames = workspace.envs()
  const currentEnvServices = await workspace.state().existingServices()
  const otherEnvsServices = _(await Promise.all(envNames
    .filter(env => env !== workspace.currentEnv()).map(
      env => workspace.state(env).existingServices()
    ))).flatten().uniq().value()

  const [servicesNewOnlyInCurrentEnv, existingOrNewToAllEnvsServices] = _.partition(
    inputServices,
    service => !currentEnvServices.includes(service) && otherEnvsServices.includes(service)
  )
  // We have a first fetch of a service in a multi-env setup.
  // The recommended practice here is to initiate the new service
  // by making an isolated fetch for the new services only.
  if (!force && shouldRecommendIsolatedModeForNewServices(
    servicesNewOnlyInCurrentEnv,
    existingOrNewToAllEnvsServices,
    envNames,
    inputIsolated
  )) {
    outputLine(formatApproveIsolatedModePrompt(
      servicesNewOnlyInCurrentEnv,
      existingOrNewToAllEnvsServices,
      inputIsolated
    ))
    if (await approveIsolatedMode(
      servicesNewOnlyInCurrentEnv,
      existingOrNewToAllEnvsServices,
      inputIsolated
    )) {
      return { services: servicesNewOnlyInCurrentEnv, isolated: true }
    }
  }

  return { services: inputServices, isolated: inputIsolated }
}

export const fetchCommand = async (
  {
    workspace, force, interactive, inputIsolated = false,
    getApprovedChanges, shouldUpdateConfig, inputServices,
    cliTelemetry, output, fetch, approveIsolatedMode, shouldCalcTotalSize,
    stateOnly,
  }: FetchCommandArgs): Promise<CliExitCode> => {
  const outputLine = (text: string): void => output.stdout.write(`${text}\n`)
  const progressOutputer = (
    startText: string,
    successText: string,
    defaultErrorText: string
  ) => (progress: StepEmitter) => {
    outputLine(EOL)
    outputLine(formatStepStart(startText))
    progress.on('completed', () => outputLine(formatStepCompleted(successText)))
    progress.on('failed', (errorText?: string) => {
      outputLine(formatStepFailed(errorText ?? defaultErrorText))
      outputLine(EOL)
    })
  }
  const workspaceTags = await getWorkspaceTelemetryTags(workspace)
  cliTelemetry.start(workspaceTags)
  const fetchProgress = new EventEmitter<FetchProgressEvents>()
  fetchProgress.on('adaptersDidInitialize', () => {
    outputLine(formatFetchHeader())
  })

  fetchProgress.on('stateWillBeUpdated', (
    progress: StepEmitter,
    changes: readonly FetchChange[]
  ) => progressOutputer(
    formatStateChanges(changes),
    Prompts.STATE_ONLY_UPDATE_END,
    Prompts.STATE_ONLY_UPDATE_FAILED(changes)
  )(progress))

  fetchProgress.on('changesWillBeFetched', (progress: StepEmitter, adapters: string[]) => progressOutputer(
    Prompts.FETCH_GET_CHANGES_START(adapters),
    Prompts.FETCH_GET_CHANGES_FINISH(adapters),
    Prompts.FETCH_GET_CHANGES_FAIL
  )(progress))

  fetchProgress.on('diffWillBeCalculated', progressOutputer(
    Prompts.FETCH_CALC_DIFF_START,
    Prompts.FETCH_CALC_DIFF_FINISH,
    Prompts.FETCH_CALC_DIFF_FAIL,
  ))
  fetchProgress.on('workspaceWillBeUpdated', (progress: StepEmitter, changes: number, approved: number) =>
    progressOutputer(
      formatChangesSummary(changes, approved),
      Prompts.FETCH_UPDATE_WORKSPACE_SUCCESS,
      Prompts.FETCH_UPDATE_WORKSPACE_FAIL
    )(progress))

  const applyChangesToWorkspace = async (
    allChanges: FetchChange[],
    isIsolated: boolean
  ): Promise<boolean> => {
    const changesToApply = force || (await workspace.isEmpty())
      ? allChanges
      : await getApprovedChanges(allChanges, interactive)

    cliTelemetry.changesToApply(changesToApply.length, workspaceTags)
    const updatingWsEmitter = new StepEmitter()
    fetchProgress.emit('workspaceWillBeUpdated', updatingWsEmitter, allChanges.length, changesToApply.length)
    const success = await updateWorkspace(workspace, output, changesToApply, isIsolated)
    if (success) {
      updatingWsEmitter.emit('completed')
      if (shouldCalcTotalSize) {
        const totalSize = await workspace.getTotalSize()
        log.debug(`Total size of the workspace is ${totalSize} bytes`)
        cliTelemetry.workspaceSize(totalSize, workspaceTags)
      }
      return true
    }
    updatingWsEmitter.emit('failed')
    return false
  }

  const applyChangesToState = async (allChanges: readonly FetchChange[]): Promise<boolean> => {
    const updatingStateEmitter = new StepEmitter()
    fetchProgress.emit('stateWillBeUpdated', updatingStateEmitter, allChanges)
    await updateStateOnly(workspace, allChanges)
    updatingStateEmitter.emit('completed')
    return true
  }

  const { services, isolated } = await getRelevantServicesAndIsolatedMode(
    inputServices || [...workspace.services()],
    workspace,
    inputIsolated,
    force,
    approveIsolatedMode,
    outputLine
  )

  const fetchResult = await fetch(
    workspace,
    fetchProgress,
    services,
  )
  if (fetchResult.success === false) {
    output.stderr.write(formatFatalFetchError(fetchResult.mergeErrors))
    cliTelemetry.failure(workspaceTags)
    return CliExitCode.AppError
  }

  // A few merge errors might have occurred,
  // but since it's fetch flow, we omitted the elements
  // and only print the merge errors
  if (!_.isEmpty(fetchResult.mergeErrors)) {
    log.debug(`fetch had ${fetchResult.mergeErrors} merge errors`)
    cliTelemetry.mergeErrors(fetchResult.mergeErrors.length, workspaceTags)
    output.stderr.write(formatMergeErrors(fetchResult.mergeErrors))
  }

  if (!_.isUndefined(fetchResult.configChanges)) {
    const abortRequests = await series(
      wu(fetchResult.configChanges.itemsByEvalOrder()).map(change => async () => {
        const newConfig = getChangeElement(change.parent())
        const adapterName = newConfig.elemID.adapter
        if (!isInstanceElement(newConfig)) {
          log.error('Got non instance config from adapter %s - %o', adapterName, newConfig)
          return false
        }
        const shouldWriteToConfig = force || await shouldUpdateConfig(
          output,
          fetchResult.adapterNameToConfigMessage?.[adapterName] || '',
          change,
        )
        if (shouldWriteToConfig) {
          await workspace.updateServiceConfig(adapterName, newConfig)
        }
        return !shouldWriteToConfig
      })
    )

    if (_.some(abortRequests)) {
      return CliExitCode.UserInputError
    }
  }

  // Unpack changes to array so we can iterate on them more than once
  const changes = [...fetchResult.changes]
  cliTelemetry.changes(changes.length, workspaceTags)

  const updatingWsSucceeded = stateOnly
    ? await applyChangesToState(changes)
    : await applyChangesToWorkspace(changes, isolated)
  // If the workspace starts empty there is no point in showing a huge amount of changes
  if (updatingWsSucceeded) {
    outputLine(formatFetchFinish())
    cliTelemetry.success(workspaceTags)
    return CliExitCode.Success
  }
  cliTelemetry.failure(workspaceTags)
  return CliExitCode.AppError
}

export const command = (
  workspaceDir: string,
  force: boolean,
  interactive: boolean,
  telemetry: Telemetry,
  output: CliOutput,
  spinnerCreator: SpinnerCreator,
  inputIsolated: boolean,
  shouldCalcTotalSize: boolean,
  inputServices?: string[],
  inputEnvironment?: string,
  stateOnly = false
): CliCommand => ({
  async execute(): Promise<CliExitCode> {
    log.debug(`running fetch command on '${workspaceDir}' [force=${force}, interactive=${
      interactive}, isolated=${inputIsolated}], environment=${inputEnvironment}, services=${inputServices}`)

    const cliTelemetry = getCliTelemetry(telemetry, 'fetch')
    const { workspace, errored } = await loadWorkspace(workspaceDir, output,
      { force, printStateRecency: true, spinnerCreator, sessionEnv: inputEnvironment })
    if (errored) {
      cliTelemetry.failure()
      return CliExitCode.AppError
    }

    return fetchCommand({
      workspace,
      force,
      interactive,
      cliTelemetry,
      output,
      fetch: apiFetch,
      getApprovedChanges: cliGetApprovedChanges,
      shouldUpdateConfig: cliShouldUpdateConfig,
      approveIsolatedMode: cliApproveIsolatedMode,
      inputServices,
      inputIsolated,
      shouldCalcTotalSize,
      stateOnly,
    })
  },
})

type FetchArgs = {
  force: boolean
  interactive: boolean
  isolated: boolean
  stateOnly: boolean
} & ServicesArgs & EnvironmentArgs
type FetchParsedCliInput = ParsedCliInput<FetchArgs>

const fetchBuilder = createCommandBuilder({
  options: {
    command: 'fetch',
    description: 'Syncs this workspace\'s NaCl files with the services\' current state',
    keyed: {
      force: {
        alias: ['f'],
        describe: 'Accept all incoming changes, even if there\'s a conflict with local changes',
        boolean: true,
        default: false,
        demandOption: false,
      },
      interactive: {
        alias: ['i'],
        describe: 'Interactively approve every incoming change',
        boolean: true,
        default: false,
        demandOption: false,
      },
      isolated: {
        alias: ['t'],
        describe: 'Restrict fetch from modifying common configuration '
          + '(might result in changes in other env folders)',
        boolean: true,
        default: false,
        demandOption: false,
      },
      'state-only': {
        alias: ['o'],
        describe: 'Fetch remote changes to the state file without mofifying the NaCL files. ',
        boolean: true,
        default: false,
        demandOption: false,
      },
    },
  },

  filters: [servicesFilter, environmentFilter],

  async build(input: FetchParsedCliInput, output: CliOutput, spinnerCreator: SpinnerCreator) {
    return command(
      '.',
      input.args.force,
      input.args.interactive,
      input.telemetry,
      output,
      spinnerCreator,
      input.args.isolated,
      input.config.shouldCalcTotalSize,
      input.args.services,
      input.args.env,
      input.args.stateOnly
    )
  },
})

export default fetchBuilder
