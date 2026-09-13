import assert from 'node:assert/strict'
import test from 'node:test'
import { caddyTemplateError, readCaddyTemplate } from '../features/environment/caddy-template.ts'

// QA: HTTP-03; local template input, not Controller rendering or serving policy.
// Rationale: the Console preserves the complete file and native placeholders;
// only the Controller resolves reserved GP references against declared Routes.
test('complete Caddyfile input preserves policy and does not require an aggregate marker', async () => {
  const policy = '\ufeffhttp://{gp.route:api.example.com:/:host} {\n' +
    '  respond /internal/* 404\n  reverse_proxy {gp.route:api.example.com:/:upstream}\n' +
    '  header X-Original-Host {host}\n}\n'
  assert.equal(caddyTemplateError(policy), null)
  assert.equal(caddyTemplateError(''), null)
  assert.equal(await readCaddyTemplate(new Blob([policy])), policy)
})

// QA: HTTP-04, UI-03; local byte validation before dispatch, not runtime rejection.
// Rationale: byte limits and UTF-8/NUL validation apply to typed and uploaded
// templates before dispatch, including files whose reported size is wrong.
test('Caddyfile rejects oversized, NUL and invalid UTF-8 input', async () => {
  assert.match(caddyTemplateError('é'.repeat(16385)), /32 KiB/)
  assert.equal(caddyTemplateError('a'.repeat(32768)), null)
  assert.match(caddyTemplateError('a\0b'), /NUL/)
  await assert.rejects(readCaddyTemplate(new Blob([Uint8Array.from([0xff])])), /UTF-8/)
  await assert.rejects(readCaddyTemplate(new Blob(['a\0b'])), /NUL/)
  await assert.rejects(readCaddyTemplate({ size: 0, arrayBuffer: async () => new ArrayBuffer(32769) }), /32 KiB/)
  let read = false
  await assert.rejects(readCaddyTemplate({ size: 32769, arrayBuffer: async () => { read = true } }), /32 KiB/)
  assert.equal(read, false)
})
