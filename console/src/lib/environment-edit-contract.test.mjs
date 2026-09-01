import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

test('Environment edit remains the strict synchronous network_pool operation', async () => {
  const document = JSON.parse(await readFile(new URL('../../../openapi.json', import.meta.url), 'utf8'))
  const operation = document.paths['/environments/{id}'].patch
  assert.equal(operation.operationId, 'environment.edit')
  assert.deepEqual(Object.keys(operation.requestBody.content), ['application/json'])
  assert.ok(operation.parameters.some((parameter) => (
    parameter.in === 'header' && parameter.name === 'Idempotency-Key' && parameter.required
  )))
  assert.ok(operation.responses['200'].content['application/json'])
  const reference = operation.requestBody.content['application/json'].schema.$ref
  assert.equal(reference, '#/components/schemas/EnvironmentEdit')
  const schema = document.components.schemas.EnvironmentEdit
  assert.deepEqual(schema.required, ['network_pool'])
  assert.deepEqual(Object.keys(schema.properties).filter((name) => name !== '$schema'), ['network_pool'])
  assert.equal(schema.additionalProperties, false)
})
