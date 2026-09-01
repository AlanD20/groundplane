export function formatAgentLabels(labels: Record<string, string>): string[] {
  return Object.entries(labels)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
}

export function formatLastReportAt(value: string | null, now = Date.now()): string {
  if (value === null) return 'Never'

  const reportedAt = Date.parse(value)
  if (!Number.isFinite(reportedAt)) return 'Never'

  const elapsedSeconds = Math.max(0, Math.floor((now - reportedAt) / 1000))
  if (elapsedSeconds < 60) return elapsedSeconds < 5 ? 'just now' : `${elapsedSeconds}s ago`

  const elapsedMinutes = Math.floor(elapsedSeconds / 60)
  if (elapsedMinutes < 60) return `${elapsedMinutes}m ago`

  const elapsedHours = Math.floor(elapsedMinutes / 60)
  if (elapsedHours < 24) return `${elapsedHours}h ago`

  return `${Math.floor(elapsedHours / 24)}d ago`
}

export function parseAgentLabels(value: string): Record<string, string> {
  const labels: Record<string, string> = {}
  for (const candidate of value.split(',').map((label) => label.trim()).filter(Boolean)) {
    const separator = candidate.indexOf('=')
    if (separator < 1) throw new Error('Agent labels must use key=value')
    const key = candidate.slice(0, separator).trim()
    const labelValue = candidate.slice(separator + 1).trim()
    if (!key) throw new Error('Agent labels must use key=value')
    if (Object.hasOwn(labels, key)) throw new Error(`Agent label key is duplicated: ${key}`)
    labels[key] = labelValue
  }
  return labels
}
