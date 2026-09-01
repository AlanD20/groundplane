import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

const ULID_ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

// ULID-style id: 48-bit millisecond timestamp (10 chars — chronologically
// sortable, so etcd ranges, deploy history, and the activity journal order
// for free) + 80 bits of CSPRNG randomness (16 chars — collision-safe).
// Sub-microsecond; the prefix keeps the kind readable (`env_…`, `svc_…`).
// IDs are ALWAYS randomly generated, never derived from names or slugs.
export function newULID(): string {
  let time = ''
  let ts = Date.now()
  for (let i = 0; i < 10; i++) {
    time = ULID_ALPHABET[ts % 32] + time
    ts = Math.floor(ts / 32)
  }
  const bytes = crypto.getRandomValues(new Uint8Array(10))
  let rand = ''
  for (let index = 0; index < 16; index++) {
    const bitIndex = index * 5
    const byteIndex = bitIndex >>> 3
    const offset = bitIndex & 7
    const pair = (bytes[byteIndex] << 8) | (bytes[byteIndex + 1] ?? 0)
    rand += ULID_ALPHABET[(pair >>> (11 - offset)) & 31]
  }
  return time + rand
}

export function newId(prefix: string): string {
  return `${prefix}_${newULID().toLowerCase()}`
}
