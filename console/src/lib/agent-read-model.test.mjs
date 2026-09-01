import assert from 'node:assert/strict'
import test from 'node:test'
import { parseAgentLabels } from './agent-read-model.ts'

test('Agent labels preserve keyed values and reject ambiguous input', () => {
  assert.deepEqual(parseAgentLabels('arch=arm64, zone=edge'), { arch: 'arm64', zone: 'edge' })
  assert.throws(() => parseAgentLabels('arch'), /key=value/)
  assert.throws(() => parseAgentLabels('arch=arm64,arch=amd64'), /duplicated/)
})
