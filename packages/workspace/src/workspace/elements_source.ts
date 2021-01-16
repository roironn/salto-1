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
import { Element, ElemID, Value, BuiltinTypesByFullName, ListType, MapType, isContainerType } from '@salto-io/adapter-api'
import { collections } from '@salto-io/lowerdash'
import { resolvePath } from '@salto-io/adapter-utils'
import { RemoteMap, InMemoryRemoteMap } from './remote_map'
import { Keywords } from '../parser/language'

const { awu } = collections.asynciterable

type ThenableIterable<T> = collections.asynciterable.ThenableIterable<T>

export interface ElementsSource {
  list(): Promise<AsyncIterable<ElemID>>
  has(id: ElemID): Promise<boolean>
  get(id: ElemID): Promise<Element | Value>
  getAll(): Promise<AsyncIterable<Element>>
  flush(): Promise<void>
  clear(): Promise<void>
  rename(name: string): Promise<void>
  set(element: Element): Promise<void>
  delete(id: ElemID): Promise<void>
}

export class InMemoryRemoteElementSource implements ElementsSource {
  private elements: RemoteMap<Element>

  constructor(elementsMap: RemoteMap<Element>) {
    this.elements = elementsMap
  }

  private async getContainerType(fullName: string): Promise<Value> {
    if (fullName.startsWith(Keywords.LIST_PREFIX) && fullName.endsWith(Keywords.GENERICS_SUFFIX)) {
      const innerElem = await this.get(
        ElemID.fromFullName(fullName.substring(
          Keywords.LIST_PREFIX.length,
          fullName.length - Keywords.GENERICS_SUFFIX.length
        ))
      )
      if (innerElem === undefined) {
        return undefined
      }
      return new ListType(innerElem)
    }
    if (fullName.startsWith(Keywords.MAP_PREFIX) && fullName.endsWith(Keywords.GENERICS_SUFFIX)) {
      const innerElem = await this.get(
        ElemID.fromFullName(fullName.substring(
          Keywords.MAP_PREFIX.length,
          fullName.length - Keywords.GENERICS_SUFFIX.length
        ))
      )
      if (innerElem === undefined) {
        return undefined
      }
      return new MapType(innerElem)
    }
    throw new Error('Not a container type')
  }

  async list(): Promise<AsyncIterable<ElemID>> {
    return awu(this.elements.keys()).map(fullname => ElemID.fromFullName(fullname))
  }

  async getAll(): Promise<AsyncIterable<Element>> {
    return this.elements.values()
  }

  async get(id: ElemID): Promise<Value | undefined> {
    const elemFullName = id.getFullName()
    if (BuiltinTypesByFullName[elemFullName] !== undefined) {
      return BuiltinTypesByFullName[elemFullName]
    }
    if ((elemFullName.startsWith(Keywords.LIST_PREFIX)
      || elemFullName.startsWith(Keywords.MAP_PREFIX))
     && (elemFullName.endsWith(Keywords.GENERICS_SUFFIX))) {
      return this.getContainerType(elemFullName)
    }
    const { parent } = id.createTopLevelParentID()
    const topLevel = await this.elements.get(parent.getFullName())
    return topLevel && resolvePath(topLevel, id)
  }

  async set(element: Element): Promise<void> {
    if (!isContainerType(element)) {
      await this.elements.set(element.elemID.getFullName(), element)
    }
  }

  async delete(id: ElemID): Promise<void> {
    await this.elements.delete(id.getFullName())
  }

  async setAll(elements: ThenableIterable<Element>): Promise<void> {
    await this.elements.setAll(
      awu(elements)
        .filter(element => !isContainerType(element))
        .map(e => [e.elemID.getFullName(), e])
    )
  }

  async has(id: ElemID): Promise<boolean> {
    return this.elements.has(id.getFullName())
  }

  async overide(elements: ThenableIterable<Element>): Promise<void> {
    await this.elements.clear()
    await this.setAll(elements)
  }

  // eslint-disable-next-line class-methods-use-this
  flush(): Promise<void> {
    throw new Error('Method not implemented.')
  }

  // eslint-disable-next-line class-methods-use-this
  clear(): Promise<void> {
    throw new Error('Method not implemented.')
  }

  // eslint-disable-next-line class-methods-use-this
  rename(_name: string): Promise<void> {
    throw new Error('Method not implemented.')
  }
}

export const createInMemoryElementSource = (
  elements: readonly Element[] = []
): InMemoryRemoteElementSource => {
  const inMemMap = new InMemoryRemoteMap(elements.map(e => [e.elemID.getFullName(), e]))
  return new InMemoryRemoteElementSource(inMemMap)
}
