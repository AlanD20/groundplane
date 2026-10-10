import assert from 'node:assert/strict'
import test from 'node:test'
import { loadBulkEntryValues, planBulkEntryChanges } from '../features/entry/bulk-entry-draft.ts'
import { formatBulkEnvironmentEntries, parseBulkEnvironmentEntries } from '../features/entry/bulk-entry-input.ts'

const variable = (key, overrides = {}) => ({
  id: `entry_${key}`, type: 'env', key, secret: false,
  source: { kind: 'literal', literal: 'original' }, exposure: ['web'], ...overrides,
})

// A prefilled draft must not overwrite redacted secrets, flatten references, or change exposure.
test('bulk edit loads all variable values, excludes files, and leaves untouched references unchanged', async () => {
  const calls = []
  const entries = [variable('PLAIN'), variable('SECRET', { secret: true }),
    variable('LINKED', { secret: true, source: { kind: 'secret_ref', secretRef: 'sec_1' } }),
    variable('FILE', { type: 'file', path: '/config' })]
  const draft = await loadBulkEntryValues(entries, async (id) => { calls.push(id); return 'private\nvalue' })
  assert.deepEqual(calls, ['entry_SECRET', 'entry_LINKED'])
  assert.deepEqual(draft.map((item) => item.key), ['PLAIN', 'SECRET', 'LINKED'])
  const values = parseBulkEnvironmentEntries(formatBulkEnvironmentEntries(draft))
  assert.deepEqual(planBulkEntryChanges(values, draft, { secret: false, exposure: ['all'] }),
    { batches: [], replacedReferences: [] })
})

test('changed keys keep protection and exposure; only new keys take defaults', async () => {
  const draft = await loadBulkEntryValues([variable('PLAIN'),
    variable('SECRET', { secret: true, exposure: ['worker'] }),
    variable('LINKED', { source: { kind: 'fact', attachId: 'att_1', fact: 'HOST' } })], async () => 'original')
  const changes = planBulkEntryChanges([
    { key: 'PLAIN', value: 'new' }, { key: 'SECRET', value: '' },
    { key: 'LINKED', value: 'override' }, { key: 'ADDED', value: 'new' },
  ], draft, { secret: true, exposure: ['all'] })
  assert.deepEqual(changes, { batches: [
    { entries: [{ key: 'PLAIN', value: 'new' }, { key: 'LINKED', value: 'override' }], secret: false, exposure: ['web'] },
    { entries: [{ key: 'SECRET', value: '' }], secret: true, exposure: ['worker'] },
    { entries: [{ key: 'ADDED', value: 'new' }], secret: true, exposure: ['all'] },
  ], replacedReferences: ['LINKED'] })
  assert.throws(() => planBulkEntryChanges([{ key: 'OTHER', value: 'new' }], draft,
    { secret: false, exposure: ['web'] }, ['OTHER']), /another Service/)
})

test('values round trip without damaging quotes, newlines, equals, backslashes or empty secrets', () => {
  const values = ['', 'a=b', ' leading and trailing ', 'a\r\nb\n', '"quoted"', 'C:\\new\\text']
    .map((value, index) => ({ key: `KEY_${index}`, value }))
  assert.deepEqual(parseBulkEnvironmentEntries(formatBulkEnvironmentEntries(values)), values)
  assert.throws(() => parseBulkEnvironmentEntries('KEY="unfinished'), /invalid quoted value/)
})

test('failed reveals do not produce a partially populated draft; large edits respect API batch limits', async () => {
  await assert.rejects(loadBulkEntryValues([variable('SECRET', { secret: true })], async () => {
    throw new Error('Unavailable')
  }), /Unavailable/)
  const values = Array.from({ length: 201 }, (_, index) => ({ key: `KEY_${index}`, value: 'v' }))
  const plan = planBulkEntryChanges(values, [], { secret: false, exposure: ['all'] })
  assert.deepEqual(plan.batches.map((batch) => batch.entries.length), [200, 1])
})
