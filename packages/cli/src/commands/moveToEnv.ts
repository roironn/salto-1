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
import { Element, ElemID, DetailedChange } from '@salto-io/adapter-api'
import { getPlan } from '@salto-io/core'
import wu from 'wu'
import { createCommandBuilder } from '../command_builder'
import { CliOutput, ParsedCliInput, CliCommand, CliExitCode } from '../types'
import { loadWorkspace } from '../workspace/workspace'
import { EnvironmentArgs } from './env'

type UGLYP_PARAMS = {
  before: Element[]
  after: Element[]
}
export type UGLYP = (args: UGLYP_PARAMS) => Promise<DetailedChange[]>

type InitArgs = {
    'selector': string
} & EnvironmentArgs

export const command = (
  selector: string,
  output: CliOutput,
): CliCommand => ({
  async execute(): Promise<CliExitCode> {
    const gp: UGLYP = async ({ before, after }) => {
      const plan = await getPlan({ before, after })
      return wu(plan.itemsByEvalOrder())
        .map(item => item.detailedChanges())
        .flatten().toArray()
    }
    const ret = await loadWorkspace('.', output)
    const { workspace } = ret
    const selectors = [selector].map(s => ElemID.fromFullName(s))
    if (workspace.moveToEnv) {
      await workspace.moveToEnv(selectors, gp)
      await workspace.flush()
      output.stdout.write('Moved elements\n')
      return CliExitCode.Success
    }
    output.stderr.write('OOOPPPPS NO MOVE TO COMMON\n')
    return CliExitCode.AppError
  },
})
type InitParsedCliInput = ParsedCliInput<InitArgs>

const initBuilder = createCommandBuilder({
  options: {
    command: 'moveToEnv [selector]',
    description: 'Moves a Salto element to env specific folder',
    positional: {
      selector: {
        type: 'string',
        description: 'Elem ID of the element to move to env (or regex if you feel like it)',
        default: undefined, // Prevent "default: []" in the help
      },
    },
  },

  async build(input: InitParsedCliInput, output: CliOutput) {
    return command(
      input.args.selector,
      output
    )
  },
})

export default initBuilder
