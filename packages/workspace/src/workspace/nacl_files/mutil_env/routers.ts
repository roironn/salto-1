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
import { Element, getChangeElement, ElemID, Value, DetailedChange, TypeMap, isObjectType, ObjectType, isPrimitiveType, PrimitiveType, isField, Field, isInstanceElement, InstanceElement } from '@salto-io/adapter-api'
import _ from 'lodash'
import path from 'path'
import { resolvePath, filterByID } from '@salto-io/adapter-utils'
import { ModificationDiff } from '@salto-io/dag'
import { values, promises } from '@salto-io/lowerdash'
import { ElementsSource } from '../../elements_source'
import {
  projectChange, projectElementOrValueToEnv, createAddChange, createRemoveChange,
} from './projections'
import { wrapAdditions, DetailedAddition } from '../addition_wrapper'
import { NaclFilesSource, FILE_EXTENSION, RoutingMode } from '../nacl_files_source'
import { mergeAdditionElement, mergeAdditionWithTarget } from './addition_merger'
import { mergeElements } from '../../../merger'
// import { mergeAdditionWithTarget } from './addition_merger'


const { isDefined } = values
const { mapValuesAsync } = promises.object
export interface RoutedChanges {
    primarySource?: DetailedChange[]
    commonSource?: DetailedChange[]
    secondarySources?: Record<string, DetailedChange[]>
}

export const filterByIDRegex = async <T>(
  id: ElemID, value: T,
  filters: RegExp[],
  rev = false
): Promise<T | undefined> => {
  const filterAnnotations = async (annotations: Value): Promise<Value> => (
    filterByIDRegex(id.createNestedID('attr'), annotations, filters, rev)
  )

  const filterAnnotationType = async (annoTypes: TypeMap): Promise<TypeMap> => _.pickBy(
    annoTypes,
    (_anno, annoName) => {
      const annoID = id.createNestedID('annotation').createNestedID(annoName)
      const pass = _.some(filters.map(f => f.test(annoID.getFullName())))
      if (pass) {
        return !rev
      }
      return rev
    }
  ) as TypeMap

  if (_.some(filters.map(f => f.test(id.getFullName())))) {
    return rev ? undefined : value
  }
  if (isObjectType(value)) {
    const filteredFields = (await Promise.all(
      Object.values(value.fields).map(field => filterByIDRegex(field.elemID, field, filters, rev))
    )).filter(isDefined)
    const fanno = await filterAnnotations(value.annotations)
    const fannot = await filterAnnotationType(value.annotationTypes)
    if (_.every([filteredFields, fanno, fannot], x => _.isEmpty(x))) {
      return undefined
    }
    return new ObjectType({
      elemID: value.elemID,
      annotations: fanno,
      annotationTypes: fannot,
      fields: _.keyBy(filteredFields, field => field.name),
      path: value.path,
      isSettings: value.isSettings,
    }) as Value as T
  }
  if (isPrimitiveType(value)) {
    const fanno = await filterAnnotations(value.annotations)
    const fannot = await filterAnnotationType(value.annotationTypes)
    if (_.every([fanno, fannot], x => _.isEmpty(x))) {
      return undefined
    }
    return new PrimitiveType({
      elemID: value.elemID,
      annotations: fanno,
      annotationTypes: fannot,
      primitive: value.primitive,
      path: value.path,
    }) as Value as T
  }
  if (isField(value)) {
    const fanno = await filterByIDRegex(value.elemID, value.annotations, filters, rev)
    if (_.every([fanno], x => _.isEmpty(x))) {
      return undefined
    }
    return new Field(
      value.parent,
      value.name,
      value.type,
      fanno
    ) as Value as T
  }
  if (isInstanceElement(value)) {
    const fv = await filterByIDRegex(value.elemID, value.value, filters, rev)
    const fanno = await filterByIDRegex(value.elemID, value.annotations, filters, rev)
    if (_.every([fanno, fv], x => _.isEmpty(x))) {
      return undefined
    }
    return new InstanceElement(
      value.elemID.name,
      value.type,
      fv,
      value.path,
      fanno
    ) as Value as T
  }

  if (_.isPlainObject(value)) {
    const filteredObj = _.pickBy(
      await mapValuesAsync(
        value,
        async (
          val: Value,
          key: string
        ) => filterByIDRegex(id.createNestedID(key), val, filters, rev)
      ),
      val => val !== undefined
    )
    return _.isEmpty(filteredObj) ? undefined : filteredObj as Value as T
  }
  if (_.isArray(value)) {
    const filteredArray = (await (Promise.all(value.map(
      async (item, i) => filterByIDRegex(id.createNestedID(i.toString()), item, filters, rev)
    )))).filter(item => item !== undefined)
    return _.isEmpty(filteredArray) ? undefined : filteredArray as Value as T
  }
  return rev ? value : undefined
}

const filterByFile = async (
  valueID: ElemID,
  value: Value,
  filename: string,
  source: NaclFilesSource
): Promise<Value> => filterByID(
  valueID,
  value,
  async id => !_.isEmpty((await source.getElements(filename))
    .filter(e => resolvePath(e, id) !== undefined))
)

const seperateChangeByFiles = async (
  change: DetailedChange,
  source: NaclFilesSource
): Promise<DetailedChange[]> => Promise.all(
  (await source.getSourceRanges(change.id))
    .map(range => range.filename)
    .map(async filename => {
      const pathHint = _.trimEnd(filename, FILE_EXTENSION).split(path.sep)
      if (change.action === 'add') {
        return {
          ...change,
          path: pathHint,
          data: {
            after: await filterByFile(change.id, change.data.after, filename, source),
          },
        }
      }
      if (change.action === 'remove') {
        return {
          ...change,
          path: pathHint,
          data: {
            before: await filterByFile(change.id, change.data.before, filename, source),
          },
        }
      }
      return {
        ...change,
        path: pathHint,
        data: {
          before: await filterByFile(change.id, change.data.before, filename, source),
          after: await filterByFile(change.id, change.data.after, filename, source),
        },
      }
    })
)

const createUpdateChanges = async (
  changes: DetailedChange[],
  commonSource: ElementsSource,
  targetSource: ElementsSource
): Promise<DetailedChange[]> => {
  const [nestedAdditions, otherChanges] = await promises.array.partition(
    changes,
    async change => (change.action === 'add'
        && change.id.nestingLevel > 0
        && !(await targetSource.get(change.id.createTopLevelParentID().parent)))
  )
  const modifiedAdditions = await Promise.all(_(nestedAdditions)
    .groupBy(addition => addition.id.createTopLevelParentID().parent.getFullName())
    .entries()
    .map(async ([parentID, elementAdditions]) => {
      const commonElement = await commonSource.get(ElemID.fromFullName(parentID))
      const targetElement = await targetSource.get(ElemID.fromFullName(parentID))
      if (commonElement && !targetElement) {
        return wrapAdditions(elementAdditions as DetailedAddition[], commonElement)
      }
      return elementAdditions
    })
    .value())
  return [
    ...otherChanges,
    ..._.flatten(modifiedAdditions),
  ]
}

const getMergeableParentID = (id: ElemID): {mergeableID: ElemID; path: string[]} => {
  const isListPart = (part: string): boolean => !Number.isNaN(Number(part))
  const firstListNamePart = id.getFullNameParts().findIndex(isListPart)
  if (firstListNamePart < 0) return { mergeableID: id, path: [] }
  const mergeableNameParts = id.getFullNameParts().slice(0, firstListNamePart)
  return {
    mergeableID: ElemID.fromFullNameParts(mergeableNameParts),
    path: id.getFullNameParts().slice(firstListNamePart),
  }
}

const createMergeableChange = async (
  changes: DetailedChange[],
  primarySource: NaclFilesSource,
  commonSource: NaclFilesSource
): Promise<DetailedChange> => {
  const refChange = changes[0]
  const { mergeableID } = getMergeableParentID(refChange.id)
  // If the mergeableID is a parent of the change id, we need to create
  // the mergeable change by manualy applying the change to the current
  // existing element.
  const base = await commonSource.get(mergeableID) || await primarySource.get(mergeableID)
  const baseAfter = _.cloneDeep(base)
  changes.forEach(change => {
    const changePath = getMergeableParentID(change.id).path
    if (change.action === 'remove') {
      _.unset(baseAfter, changePath)
    } else {
      _.set(baseAfter, changePath, change.data.after)
    }
  })
  return {
    action: 'modify',
    id: mergeableID,
    path: refChange.path,
    data: {
      before: base,
      after: baseAfter,
    },
  }
}

export const routeFetch = async (
  change: DetailedChange,
  primarySource: NaclFilesSource,
  commonSource: NaclFilesSource,
  _secondarySources: Record<string, NaclFilesSource>
): Promise<RoutedChanges> => {
  // If the add change projects to a secondary source we can't
  // add it to common since it is already marked as env specific.
  if (change.action === 'add') {
    // const secondaryProjections = await Promise.all(
    //   _.values(secondarySources)
    //     .map(src => projectElementOrValueToEnv(getChangeElement(change), change.id, src))
    // )
    // if (_.some(secondaryProjections)) {
    return { primarySource: [change] }
    // }
    // if (change.id.isTopLevel()) {
    //   return { commonSource: [change] }
    // }
    // // This is a new value / field / annotation addition. In this case, we will want to
    // // add it to common *unless* the entire element is env specific
    // const commonTopLevelElement =
    // await commonSource.get(change.id.createTopLevelParentID().parent)
    // return commonTopLevelElement ? { commonSource: [change] } : { primarySource: [change] }
  }
  // We add to the current defining source.
  const currentChanges = await projectChange(change, primarySource)
  const commonChanges = await projectChange(change, commonSource)
  return {
    primarySource: currentChanges,
    commonSource: commonChanges,
  }
}

const getChangePathHint = async (
  change: DetailedChange,
  commonSource: NaclFilesSource
): Promise<string[] | undefined> => {
  if (change.path) return change.path
  const refFilename = (await commonSource.getSourceRanges(change.id))
    .map(sourceRange => sourceRange.filename)[0]

  return refFilename
    ? _.trimEnd(refFilename, FILE_EXTENSION).split(path.sep)
    : undefined
}

export const routeNewEnv = async (
  change: DetailedChange,
  primarySource: NaclFilesSource,
  commonSource: NaclFilesSource,
  secondarySources: Record<string, NaclFilesSource>
): Promise<RoutedChanges> => {
  // This is an add change, which means the element is not in common.
  // so we will add it to the current action enviornment.
  const pathHint = await getChangePathHint(change, commonSource)

  if (change.action === 'add') {
    return { primarySource: [change] }
  }

  // In remove and modify changes, we need to remove the current value from
  // common, add it to the inactive envs, and apply the actual change to the
  // active env.
  const changeElement = getChangeElement(change)
  const currentEnvChanges = await projectChange(change, primarySource)
  const commonChangeProjection = await projectElementOrValueToEnv(
    changeElement,
    change.id,
    commonSource
  )
  // If the element is not in common, then we can apply the change to
  // the primary source
  if (_.isUndefined(commonChangeProjection)) {
    return { primarySource: [change] }
  }

  const currentCommonElement = await commonSource.get(change.id)
  // Keeping the parser happy, this will never happen (see above)
  if (_.isUndefined(currentCommonElement)) {
    throw Error('Missing element in common')
  }
  // Add the changed part of common to the target source
  const modifyWithCommonProj = change.action === 'modify' && !_.isUndefined(commonChangeProjection)
  const addCommonProjectionToCurrentChanges = modifyWithCommonProj
    ? await projectChange(
      createAddChange(commonChangeProjection, change.id, pathHint),
      primarySource
    ) : []
  // Add the old value of common to the inactive sources
  const secondaryChanges = _.fromPairs(
    await Promise.all(
      _.entries(secondarySources)
        .map(async ([name, source]) => [
          name,
          _.flatten(
            await Promise.all(
              (await projectChange(
                createAddChange(currentCommonElement, change.id, pathHint), source
              )
              ).map(projectedChange => seperateChangeByFiles(projectedChange, commonSource))
            )
          ),
        ])
    )
  )
  return {
    primarySource: [...currentEnvChanges, ...addCommonProjectionToCurrentChanges],
    commonSource: [createRemoveChange(commonChangeProjection, change.id, pathHint)],
    secondarySources: secondaryChanges,
  }
}

const partitionMergeableChanges = async (
  changes: DetailedChange[],
  commonSource: NaclFilesSource
): Promise<[DetailedChange[], DetailedChange[]]> => (
  promises.array.partition(
    changes,
    async change => {
      const { mergeableID } = getMergeableParentID(change.id)
      return !_.isEqual(change.id, mergeableID)
        && !_.isUndefined(await commonSource.get(mergeableID))
    }
  )
)

const toMergeableChanges = async (
  changes: DetailedChange[],
  primarySource: NaclFilesSource,
  commonSource: NaclFilesSource,
): Promise<DetailedChange[]> => {
  // First we create mergeable changes!
  // We need to modify a change iff:
  // 1) It has a common projection
  // 2) It is inside an array
  const [nonMergeableChanges, mergeableChanges] = await partitionMergeableChanges(
    changes,
    commonSource
  )
  return [
    ...mergeableChanges,
    ...await Promise.all(_(nonMergeableChanges)
      .groupBy(c => getMergeableParentID(c.id).mergeableID.getFullName())
      .values()
      .map(c => createMergeableChange(c, primarySource, commonSource))
      .value()),
  ]
}

// Returns: [envSpecific, restOfChanges]
const seperateEnvSpecificChanges = async (
  changes: DetailedChange[]
): Promise<[DetailedChange[], DetailedChange[]]> => {
  const rawDataFromFileOMGChangeThis = { envSpecificPaths: [], envSpecificIds: [] }
  // JSON.parse(readFileSync('./env_spec.json', 'utf8'))
  const envSpecificPaths = rawDataFromFileOMGChangeThis
    .envSpecificPaths
    .map((s: string) => new RegExp(s))
  const envSpecificIds = rawDataFromFileOMGChangeThis
    .envSpecificIds
    .map((s: string) => new RegExp(s))
  const [specByPath, restOfChanges] = _.partition(
    changes,
    c => {
      const pp = c.path || getChangeElement(c).path || []
      return _.some(envSpecificPaths, p => p.test(pp))
    }
  )
  const filteredEnvSpecific = (await Promise.all(restOfChanges.map(
    async (c): Promise<DetailedChange> => {
    // This is VERY UGLY but it works for now.
      const cAsMod = c as ModificationDiff<Value>
      return {
        id: c.id,
        path: c.path,
        action: cAsMod.action,
        data: {
          after: await filterByIDRegex(
            c.id,
            cAsMod.data.after,
            envSpecificIds
          ),
          before: await filterByIDRegex(
            c.id,
            cAsMod.data.before,
            envSpecificIds
          ),
        },
      }
    }
  ))).filter((c: Value) => c.data.after !== undefined || c.data.before !== undefined)

  const filteredRestOfChanges = (await Promise.all(restOfChanges.map(
    async (c): Promise<DetailedChange> => {
      // This is VERY UGLY but it works for now.
      const cAsMod = c as ModificationDiff<Value>
      return {
        id: c.id,
        path: c.path,
        action: cAsMod.action,
        data: {
          after: await filterByIDRegex(
            c.id,
            cAsMod.data.after,
            envSpecificIds,
            true
          ),
          before: await filterByIDRegex(
            c.id,
            cAsMod.data.before,
            envSpecificIds,
            true
          ),
        },
      }
    }
  ))).filter((c: Value) => c.data.after !== undefined || c.data.before !== undefined)

  return [
    [...specByPath, ...filteredEnvSpecific],
    filteredRestOfChanges,
  ]
}

export const routeChanges = async (
  rawChanges: DetailedChange[],
  primarySource: NaclFilesSource,
  commonSource: NaclFilesSource,
  secondarySources: Record<string, NaclFilesSource>,
  mode?: RoutingMode
): Promise<RoutedChanges> => {
  const isIsolated = mode === 'isolated'
  // const [envSpecificChanges, restOfChanges] = await seperateEnvSpecificChanges(rawChanges)
  const changes = isIsolated
    ? await toMergeableChanges(rawChanges, primarySource, commonSource)
    : rawChanges
  const routedChanges = await Promise.all(changes.map(c => (isIsolated
    ? routeNewEnv(c, primarySource, commonSource, secondarySources)
    : routeFetch(c, primarySource, commonSource, secondarySources))))
  const secondaryEnvsChanges = _.mergeWith(
    {},
    ...routedChanges.map(r => r.secondarySources || {}),
    (objValue: DetailedChange[], srcValue: DetailedChange[]) => (
      objValue ? [...objValue, ...srcValue] : srcValue
    )
  ) as Record<string, DetailedChange[]>
  return {
    primarySource: await createUpdateChanges(
      _.flatten(routedChanges.map(r => r.primarySource || [])),
      commonSource,
      primarySource
    ),
    commonSource: await createUpdateChanges(
      _.flatten(routedChanges.map(r => r.commonSource || [])),
      commonSource,
      commonSource
    ),
    secondarySources: await promises.object.mapValuesAsync(
      secondaryEnvsChanges,
      (srcChanges, srcName) => createUpdateChanges(
        srcChanges,
        commonSource,
        secondarySources[srcName]
      )
    ),
  }
}
type UGLYP_PARAMS = {
  before: Element[]
  after: Element[]
}
export type UGLYP = (args: UGLYP_PARAMS) => Promise<DetailedChange[]>

const mergeWithTargetElem = async (
  changes: DetailedAddition[],
  source: NaclFilesSource,
  getPlan: UGLYP
): Promise<DetailedChange[]> => {
  const r = await Promise.all(_(changes)
    .groupBy(c => c.id.createTopLevelParentID().parent.getFullName() + c.path)
    .mapValues(async (elemChanges, _fullName) => {
      const c = elemChanges[0]
      // const elem = await source.get(ElemID.fromFullName(fullName)) as Element
      const fullName = c.id.createTopLevelParentID().parent.getFullName()
      const filename = `${(c.path || getChangeElement(c).path || []).join('/')}.nacl`
      const elem = (await source.getElements(filename) || [])
        .find(e => e.elemID.getFullName() === fullName)
      if (!elem) return elemChanges
      const [topLevelChange, restOfChanges] = _.partition(elemChanges, cc => cc.id.isTopLevel())
      const merged = mergeElements([
        ...topLevelChange.map(cc => cc.data.after),
        elem,
      ])
      const mergedForReal = _.isEmpty(restOfChanges)
        ? merged.merged[0]
        : mergeAdditionElement(restOfChanges, merged.merged[0])
      const newChanges = await getPlan({
        before: [elem],
        after: [mergedForReal],
      })
      return newChanges.map(cc => ({ ...cc, path: c.path }))
    })
    .values()
    .value())
  return _.flatten(r)
}

export const moveToEnv = async (
  selectors: ElemID[],
  primarySource: NaclFilesSource,
  commonSource: NaclFilesSource,
  secondarySources: Record<string, NaclFilesSource>,
  getPlan: UGLYP
): Promise<RoutedChanges> => {
  const routedChanges = await Promise.all(
    selectors.map(async (s): Promise<RoutedChanges> => {
      const currentCommonElement = await commonSource.get(s)
      const primChanges = _.flatten(await Promise.all((
        await projectChange(createAddChange(currentCommonElement, s), primarySource)
      ).map(projectedChange => seperateChangeByFiles(projectedChange, commonSource))))

      const secChanges = _.fromPairs(
        await Promise.all(
          _.entries(secondarySources)
            .map(async ([name, source]) => [
              name,
              _.flatten(
                await Promise.all(
                  (await projectChange(
                    createAddChange(currentCommonElement, s), source
                  )
                  ).map(projectedChange => seperateChangeByFiles(projectedChange, commonSource))
                )
              ),
            ])
        )
      )

      return {
        commonSource: [createRemoveChange(currentCommonElement, s)],
        primarySource: primChanges,
        secondarySources: secChanges,
      }
    })
  )

  const secondaryEnvsChanges = _.mergeWith(
    {},
    ...routedChanges.map(r => r.secondarySources || {}),
    (objValue: DetailedChange[], srcValue: DetailedChange[]) => (
      objValue ? [...objValue, ...srcValue] : srcValue
    )
  ) as Record<string, DetailedChange[]>

  return {
    primarySource: await mergeWithTargetElem(await createUpdateChanges(
      _.flatten(routedChanges.map(r => r.primarySource || [])),
      commonSource,
      primarySource
    ) as DetailedAddition[], primarySource, getPlan),
    commonSource: await createUpdateChanges(
      _.flatten(routedChanges.map(r => r.commonSource || [])),
      commonSource,
      commonSource
    ),
    secondarySources: await promises.object.mapValuesAsync(
      secondaryEnvsChanges,
      async (srcChanges, srcName) => mergeWithTargetElem(await createUpdateChanges(
        srcChanges,
        commonSource,
        secondarySources[srcName]
      ) as DetailedAddition[], secondarySources[srcName], getPlan)
    ),
  }
}

export const moveToCommon = async (
  selectors: ElemID[],
  primarySource: NaclFilesSource,
  commonSource: NaclFilesSource,
  secondarySources: Record<string, NaclFilesSource>,
  getPlan: UGLYP
): Promise<RoutedChanges> => {
  const getSourceChange = async (
    bid: ElemID,
    valueToRemove: Value,
    source: NaclFilesSource,
    gp: UGLYP
  ): Promise<DetailedChange[][]> => {
    const topLevelID = bid.createTopLevelParentID().parent
    const before = await source.get(topLevelID)
    const rawChanges = await seperateChangeByFiles(createAddChange(valueToRemove, bid), source)
    const [envSpecificChanges, commonChanges] = await seperateEnvSpecificChanges(rawChanges)
    if (before === undefined) return [[], commonChanges]
    const [topLevelChanges, restOfChanges] = _.partition(
      envSpecificChanges,
      cc => cc.id.isTopLevel()
    )
    const mergedBase = mergeElements(topLevelChanges.map(((c: Value) => c.data.after))).merged[0]
    const after = restOfChanges.length > 0
      ? (
        mergeAdditionWithTarget(restOfChanges as DetailedAddition[], mergedBase).data as Value
      ).after
      : mergedBase
    const sourceChanges = await gp({
      before: before === undefined ? [] : [before],
      after: after === undefined ? [] : [after],
    })
    return [sourceChanges, commonChanges]
  }

  const routedChanges = await Promise.all(
    selectors.map(async (s): Promise<RoutedChanges> => {
      const primaryCommonElement = await primarySource.get(s)
      // const commonChangesAll = _.flatten(await Promise.all((
      //   await projectChange(createAddChange(primaryCommonElement, s), commonSource)
      // ).map(projectedChange => seperateChangeByFiles(projectedChange, primarySource))))
      // const [commonChanges, needToBeModify] = await seperateEnvSpecificChanges(commonChangesAll)

      const [primaryChanges, commonChanges] = await getSourceChange(
        s,
        primaryCommonElement,
        primarySource,
        getPlan
      )

      // For each change:
      // Make an add change of needs to be modieid - then - same as in move to env!
      const primChanges = _.flatten(
        await Promise.all(primaryChanges.map(c => seperateChangeByFiles(c, primarySource)))
      )
      const secChanges = _.fromPairs(
        await Promise.all(_.entries(secondarySources)
          .map(async ([name, source]) => {
            const [sourceChanges] = await getSourceChange(
              s,
              primaryCommonElement,
              source,
              getPlan
            )
            return [
              name,
              _.flatten(
                await Promise.all(sourceChanges.map(c => seperateChangeByFiles(c, source)))
              ),
            ]
          }))
      )
      return {
        commonSource: commonChanges,
        primarySource: primChanges,
        secondarySources: secChanges,
      }
    })
  )

  const secondaryEnvsChanges = _.mergeWith(
    {},
    ...routedChanges.map(r => r.secondarySources || {}),
    (objValue: DetailedChange[], srcValue: DetailedChange[]) => (
      objValue ? [...objValue, ...srcValue] : srcValue
    )
  ) as Record<string, DetailedChange[]>

  return {
    primarySource: await createUpdateChanges(
      _.flatten(routedChanges.map(r => r.primarySource || [])),
      commonSource,
      primarySource
    ),
    commonSource: await mergeWithTargetElem(await createUpdateChanges(
      _.flatten(routedChanges.map(r => r.commonSource || [])),
      commonSource,
      commonSource
    ) as DetailedAddition[], commonSource, getPlan),
    secondarySources: await promises.object.mapValuesAsync(
      secondaryEnvsChanges,
      (srcChanges, srcName) => createUpdateChanges(
        srcChanges,
        commonSource,
        secondarySources[srcName]
      )
    ),
  }
}
