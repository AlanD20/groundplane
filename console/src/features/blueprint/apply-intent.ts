import type { BlueprintApplyRequest, BlueprintBundleManifest } from '@/lib/blueprint-bundle'

type IntentIdentity = {
  version: 1
  environmentId: string
  revision: string
  key: string
}

type PendingIntent = IntentIdentity & {
  manifest: BlueprintBundleManifest
  parts: { part: string; bytes: string }[]
}

type AcceptedIntent = IntentIdentity & { taskId: string }
type SavedIntent = PendingIntent | AcceptedIntent

const storageKey = 'groundplane-blueprint-apply'

function encode(bytes: Uint8Array) {
  let binary = ''
  for (let offset = 0; offset < bytes.length; offset += 32768) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 32768))
  }
  return btoa(binary)
}

function decode(value: string) {
  const binary = atob(value)
  const bytes = new Uint8Array(binary.length)
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index)
  return bytes
}

function validIntent(value: unknown): value is SavedIntent {
  if (!value || typeof value !== 'object') return false
  const intent = value as Record<string, unknown>
  if (intent.version !== 1 || typeof intent.environmentId !== 'string' ||
      typeof intent.revision !== 'string' || typeof intent.key !== 'string') return false
  if (typeof intent.taskId === 'string') return true
  const manifest = intent.manifest as Record<string, unknown> | undefined
  return !!manifest && Array.isArray(manifest.files) && Array.isArray(intent.parts) &&
    intent.parts.every((part: unknown) => {
      if (!part || typeof part !== 'object') return false
      const file = part as Record<string, unknown>
      return typeof file.part === 'string' && typeof file.bytes === 'string'
    })
}

// A tab saves the exact bounded bundle before publication. A reload can replay
// the same bytes, revision and key; no new Apply may replace unresolved work.
export class BlueprintApplyIntent {
  #storage: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>
  #current: SavedIntent | null = null
  #loadingError: unknown
  #beginning: { environmentId: string; key: string; promise: Promise<void> } | null = null
  #publication: Promise<string> | null = null

  constructor(storage: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>) {
    this.#storage = storage
    try {
      const raw = storage.getItem(storageKey)
      if (!raw) return
      const parsed: unknown = JSON.parse(raw)
      if (!validIntent(parsed)) throw new Error('Saved Blueprint Apply request is invalid; resolve it before applying again')
      this.#current = parsed
    } catch (error) { this.#loadingError = error }
  }

  get current() {
    if (this.#loadingError) throw this.#loadingError
    if (!this.#current) return null
    const { environmentId, revision, key } = this.#current
    const taskId = 'taskId' in this.#current ? this.#current.taskId : undefined
    return { environmentId, revision, key, taskId }
  }

  begin(environmentId: string, request: BlueprintApplyRequest, revision: string, key: string): Promise<void> {
    if (this.#loadingError) throw this.#loadingError
    if (this.#current) {
      if (this.#current.environmentId === environmentId && this.#current.key === key) return Promise.resolve()
      throw new Error('An earlier Blueprint Apply is unresolved. Resolve it before starting another Apply.')
    }
    if (this.#beginning) {
      if (this.#beginning.environmentId === environmentId && this.#beginning.key === key) return this.#beginning.promise
      throw new Error('Another Blueprint Apply is being prepared. Wait for it to resolve.')
    }
    const promise = this.#capture(environmentId, request, revision, key)
      .finally(() => { this.#beginning = null })
    this.#beginning = { environmentId, key, promise }
    return promise
  }

  async #capture(environmentId: string, request: BlueprintApplyRequest, revision: string, key: string) {
    const parts = await Promise.all(request.parts.map(async ({ part, content }) => ({
      part, bytes: encode(new Uint8Array(await content.arrayBuffer())),
    })))
    this.#save({ version: 1, environmentId, revision, key, manifest: request.manifest, parts })
  }

  publish(send: (environmentId: string, request: BlueprintApplyRequest, revision: string, key: string) => Promise<{ task_id: string }>, rejected: (error: unknown) => boolean) {
    if (this.#publication) return this.#publication
    this.#publication = this.#publish(send, rejected).finally(() => { this.#publication = null })
    return this.#publication
  }

  async #publish(send: (environmentId: string, request: BlueprintApplyRequest, revision: string, key: string) => Promise<{ task_id: string }>, rejected: (error: unknown) => boolean) {
    if (this.#loadingError) throw this.#loadingError
    const intent = this.#current
    if (!intent) throw new Error('No Blueprint Apply request to resolve')
    if ('taskId' in intent) return intent.taskId
    let accepted: { task_id: string }
    try {
      const request: BlueprintApplyRequest = {
        manifest: intent.manifest,
        parts: intent.parts.map(({ part, bytes }) => ({ part, content: new File([decode(bytes)], part) })),
      }
      accepted = await send(intent.environmentId, request, intent.revision, intent.key)
    } catch (error) {
      if (rejected(error)) this.#clear()
      throw error
    }
    if (!accepted.task_id) throw new Error('Controller response is missing Blueprint Apply task_id')
    this.#save({
      version: 1, environmentId: intent.environmentId, revision: intent.revision,
      key: intent.key, taskId: accepted.task_id,
    })
    return accepted.task_id
  }

  settle(taskId: string) {
    if (this.#current && 'taskId' in this.#current && this.#current.taskId === taskId) this.#clear()
  }

  #save(intent: SavedIntent) {
    this.#storage.setItem(storageKey, JSON.stringify(intent))
    this.#current = intent
  }

  #clear() {
    this.#storage.removeItem(storageKey)
    this.#current = null
  }
}
