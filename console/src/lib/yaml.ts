// Minimal YAML serializer for the desired-state (Blueprint) views:
// 2-space indent, quotes only when needed, [] / {} for empties, null for nulls.
// Good enough to render the Blueprint documents in the Console.

function needsQuotes(v: string): boolean {
  if (v === '') return true
  if (v.includes('\n')) return true
  // quote when it starts with a digit, contains ':' or ' #', or looks like a key: value
  if (/^\d/.test(v) || /:\s|^[A-Za-z0-9_.-]+:\s*\S/.test(v)) return true
  return !/^[A-Za-z0-9_./:@+~*#\[\](){}<>|=^%$&!?'",;\\ -]+$/.test(v)
}

function scalar(v: unknown): string {
  if (v === null || v === undefined) return 'null'
  if (typeof v === 'boolean') return v ? 'true' : 'false'
  if (typeof v === 'number') return String(v)
  if (typeof v === 'string') return needsQuotes(v) ? JSON.stringify(v) : v
  return JSON.stringify(v)
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

export function toYAML(value: unknown, indent = 0): string {
  const pad = '  '.repeat(indent)
  if (Array.isArray(value)) {
    if (value.length === 0) return pad + '[]'
    const lines: string[] = []
    for (const item of value) {
      if (isPlainObject(item) || Array.isArray(item)) {
        lines.push(pad + '- ' + toYAML(item, indent + 1).trimStart())
      } else {
        lines.push(pad + '- ' + scalar(item))
      }
    }
    return lines.join('\n')
  }
  if (isPlainObject(value)) {
    const entries = Object.entries(value).filter(([, v]) => v !== undefined)
    if (entries.length === 0) return pad + '{}'
    const lines: string[] = []
    for (const [k, v] of entries) {
      if (isPlainObject(v) || Array.isArray(v)) {
        const child = toYAML(v, indent + 1)
        if (child.trim() === '[]' || child.trim() === '{}') {
          lines.push(pad + k + ': ' + child.trim())
        } else {
          lines.push(pad + k + ':')
          lines.push(child)
        }
      } else {
        lines.push(pad + k + ': ' + scalar(v))
      }
    }
    return lines.join('\n')
  }
  return pad + scalar(value)
}
