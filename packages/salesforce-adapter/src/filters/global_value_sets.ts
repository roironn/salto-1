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
import {
  Element, ObjectType, Field, ReferenceExpression, isInstanceElement,
} from '@salto-io/adapter-api'
import { collections } from '@salto-io/lowerdash'
import { FilterWith } from '../filter'
import { VALUE_SET_FIELDS, INSTANCE_FULL_NAME_FIELD } from '../constants'
import { metadataType, isCustomObject } from '../transformers/transformer'

const { awu } = collections.asynciterable

export const GLOBAL_VALUE_SET = 'GlobalValueSet'
export const CUSTOM_VALUE = 'customValue'
export const MASTER_LABEL = 'master_label'

type GlobalValueSetsLookup = Record<string, ReferenceExpression>

const getValueSetNameToRef = async (elements: Element[]): Promise<GlobalValueSetsLookup> => {
  const globalValueSets = await awu(elements)
    .filter(isInstanceElement)
    .filter(async e => await metadataType(e) === GLOBAL_VALUE_SET)
    .toArray()
  return _.fromPairs(globalValueSets
    .map(gvs => [
      gvs.value[INSTANCE_FULL_NAME_FIELD],
      new ReferenceExpression(gvs.elemID),
    ]))
}

const addGlobalValueSetRefToObject = (
  object: ObjectType,
  gvsToRef: GlobalValueSetsLookup
): void => {
  const getValueSetName = (field: Field): string | undefined =>
    field.annotations[VALUE_SET_FIELDS.VALUE_SET_NAME]

  Object.values(object.fields)
    .filter(f => getValueSetName(f))
    .forEach(f => {
      const valueSetName = getValueSetName(f)
      if (valueSetName && gvsToRef[valueSetName]) {
        f.annotations[VALUE_SET_FIELDS.VALUE_SET_NAME] = gvsToRef[valueSetName]
      }
    })
}

/**
 * Create filter that adds global value set references where needed
 */
const filterCreator = (): FilterWith<'onFetch'> => ({
  /**
   * @param elements the already fetched elements
   */
  onFetch: async (elements: Element[]): Promise<void> => {
    const valueSetNameToRef = await getValueSetNameToRef(elements)
    const customObjects = await awu(elements).filter(isCustomObject).toArray() as ObjectType[]
    customObjects.forEach(object => addGlobalValueSetRefToObject(object, valueSetNameToRef))
  },
})

export default filterCreator
