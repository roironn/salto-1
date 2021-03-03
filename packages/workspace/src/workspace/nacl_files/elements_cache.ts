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
import { Element, Change, isEqualElements, toChange, ElemID, SaltoError, ReadOnlyElementsSource, getChangeElement, getAfterFromChange } from '@salto-io/adapter-api'
import { logger } from '@salto-io/logging'
import { collections, values } from '@salto-io/lowerdash'

import _ from 'lodash'
import { MergeResult } from '../../merger'
import { ElementsSource } from '../elements_source'
import { RemoteMap } from '../remote_map'

const { awu } = collections.asynciterable
const log = logger(module)

export const buildNewMergedElementsAndErrors = async ({
  newElements,
  currentElements,
  currentErrors,
  relevantElementIDs,
  mergeFunc,
}: {
  newElements: AsyncIterable<Element>
  currentElements: ElementsSource
  currentErrors: RemoteMap<SaltoError[]>
  relevantElementIDs: AsyncIterable<ElemID>
  mergeFunc: (elements: AsyncIterable<Element>) => Promise<MergeResult>
}): Promise<Change[]> => {
  log.info('going to merge new elements to the existing elements')
  const changes: Change[] = []
  const newMergedElementsResult = await mergeFunc(newElements)
  const hasCurrentElements = !(await currentElements.isEmpty())
  const hasCurrentErrors = !(await currentErrors.isEmpty())
  if (!hasCurrentElements && !hasCurrentErrors) {
    await awu(newMergedElementsResult.merged.values()).forEach(async element => {
      changes.push(toChange({ after: element }) as Change)
      await currentElements.set(element)
    })
    await currentErrors.setAll(newMergedElementsResult.errors.entries())
    return changes
  }
  const sieve = new Set<string>()

  await awu(relevantElementIDs).forEach(async id => {
    const fullname = id.getFullName()
    if (!sieve.has(fullname)) {
      sieve.add(fullname)
      const before = await currentElements.get(id)
      const mergedItem = await newMergedElementsResult.merged.get(fullname)
      if (!isEqualElements(before, mergedItem)) {
        if (mergedItem !== undefined) {
          await currentElements.set(mergedItem)
        } else if (before !== undefined) {
          await currentElements.delete(id)
        }
        changes.push(toChange({ before, after: mergedItem }))
        const mergeErrors = await newMergedElementsResult.errors.get(fullname)
        if (mergeErrors !== undefined) {
          await currentErrors.set(fullname, mergeErrors)
        }
      }
    }
  })
  return changes
}

export const getBuildMergeData = async ({
  src1Changes,
  src2Changes,
  src1,
  src2,
}: {
  src1Changes: Change<Element>[]
  src2Changes: Change<Element>[]
  src1: ReadOnlyElementsSource
  src2: ReadOnlyElementsSource
}): Promise<{
  newElements: AsyncIterable<Element>
  relevantElementIDs: AsyncIterable<ElemID>
}> => {
  const relevantElementIDs = _.uniqBy(
    [src1Changes, src2Changes]
      .flat()
      .map(getChangeElement)
      .map(e => e.elemID),
    id => id.getFullName()
  )

  const src1ChangesByID = _.keyBy(
    src1Changes,
    change => getChangeElement(change).elemID.getFullName()
  )
  const src2ChangesByID = _.keyBy(
    src2Changes,
    change => getChangeElement(change).elemID.getFullName()
  )

  const changeElements = [...src1Changes, ...src2Changes]
    .map(getAfterFromChange)
    .filter(values.isDefined)

  const unmodifiedFragments = awu(relevantElementIDs).map(async id => {
    const sr1Change = src1ChangesByID[id.getFullName()]
    const src2Change = src2ChangesByID[id.getFullName()]
    if (values.isDefined(sr1Change) && values.isDefined(src2Change)) {
      return undefined
    }
    return values.isDefined(sr1Change)
      ? src2.get(id)
      : src1.get(id)
  }).filter(values.isDefined)
  return {
    newElements: awu(changeElements).concat(unmodifiedFragments),
    relevantElementIDs: awu(relevantElementIDs),
  }
}
