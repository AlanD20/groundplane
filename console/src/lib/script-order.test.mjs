import assert from 'node:assert/strict'
import test from 'node:test'
import { scriptFromAPI } from './script-api.ts'
import { parseScriptOrder } from '../features/script/script-order.ts'

const script = {
  id: 'scr_one', environment_id: 'env_one', slug: 'migrate', service_id: 'svc_one', service: 'api',
  script: 'echo migrate', when: 'pre-deploy', order: 10, origin: 'api', active_generation: 1,
  execution: { mode: 'inherited' },
}

// Rationale: the Console shows the Controller's exact bounded order, including
// zero, and refuses malformed data instead of presenting a fabricated default.
test('Script response retains numeric order and rejects malformed values', () => {
  for (const order of [0, 10, 65535]) assert.equal(scriptFromAPI({ ...script, order }).order, order)
  for (const order of [-1, 65536, 1.5, null, undefined, '10', NaN]) {
    assert.throws(() => scriptFromAPI({ ...script, order }), /order/)
  }
})

// Rationale: the editor permits zero and the maximum but never silently coerces
// blank, fractional, negative, scientific-notation or out-of-range authoring.
test('Script order input has an explicit bounded integer decision', () => {
  for (const value of ['0', '10', '65535']) assert.equal(parseScriptOrder(value), Number(value))
  for (const value of ['', ' ', '-1', '65536', '1.5', '1e1', 'Infinity']) {
    assert.equal(parseScriptOrder(value), undefined)
  }
})
