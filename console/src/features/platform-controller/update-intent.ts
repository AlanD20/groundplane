type Intent = { release: string; idempotencyKey: string; taskId?: string }
type IntentStorage = Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>
type Publisher = (release: string, key: string) => Promise<{ task_id: string }>
const storageKey = 'groundplane-controller-update'

// One tab retains its protected request before publication, then its accepted
// Task. A reload may resolve this request but never invents a replacement key.
export class ControllerUpdateIntent {
  #storage: IntentStorage
  #current: Intent | null = null
  #loadingError: unknown
  #publication: Promise<string> | null = null

  constructor(storage: IntentStorage) {
    this.#storage = storage
    try {
      const raw = storage.getItem(storageKey)
      if (!raw) return
      const saved: unknown = JSON.parse(raw)
      if (!validIntent(saved)) throw new Error('Saved Controller update request is invalid; resolve it before updating')
      this.#current = saved
    } catch (error) { this.#loadingError = error }
  }

  get current(): Intent | null { return this.#current ? { ...this.#current } : null }

  begin(release: string, idempotencyKey: string) {
    if (this.#loadingError) throw this.#loadingError
    if (this.#current) throw new Error('A Controller update request is unresolved')
    const intent = { release, idempotencyKey }
    if (!validIntent(intent)) throw new Error('Controller update requires a pinned release and idempotency key')
    this.#save(intent)
  }

  publish(send: Publisher, rejected: (error: unknown) => boolean): Promise<string> {
    if (this.#publication) return this.#publication
    this.#publication = this.#publish(send, rejected).finally(() => { this.#publication = null })
    return this.#publication
  }

  async #publish(send: Publisher, rejected: (error: unknown) => boolean): Promise<string> {
    const intent = this.#current
    if (!intent) throw new Error('No Controller update request to resolve')
    if (intent.taskId) return intent.taskId
    let accepted: { task_id: string }
    try { accepted = await send(intent.release, intent.idempotencyKey) }
    catch (error) {
      if (rejected(error)) this.#clear()
      throw error
    }
    if (!accepted.task_id) throw new Error('Controller response is missing update task_id')
    // Even if persisting acceptance fails, retain the original key for replay.
    this.#save({ ...intent, taskId: accepted.task_id })
    return accepted.task_id
  }

  settle(taskId: string) {
    if (this.#current?.taskId === taskId) this.#clear()
  }

  #save(intent: Intent) {
    this.#storage.setItem(storageKey, JSON.stringify(intent))
    this.#current = intent
  }

  #clear() {
    this.#storage.removeItem(storageKey)
    this.#current = null
  }
}

function validIntent(value: unknown): value is Intent {
  return value !== null && typeof value === 'object' &&
    'release' in value && typeof value.release === 'string' && /^sha256:[a-f0-9]{64}$/.test(value.release) &&
    'idempotencyKey' in value && typeof value.idempotencyKey === 'string' && /^[0-7][0-9A-HJKMNP-TV-Z]{25}$/.test(value.idempotencyKey) &&
    (!('taskId' in value) || (typeof value.taskId === 'string' && value.taskId.startsWith('task_')))
}
