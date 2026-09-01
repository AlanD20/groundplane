export function isAuthoritativeTaskUnavailable(error: unknown) {
  if (!error || typeof error !== 'object') return false
  const candidate = error as { status?: unknown; code?: unknown }
  return candidate.status === 404 || candidate.code === 'not_found'
}

export function isDefinitiveRemovalRequestRejection(error: unknown) {
  if (!error || typeof error !== 'object') return false
  const status = (error as { status?: unknown }).status
  return typeof status === 'number' && status >= 400 && status < 500 && status !== 408 && status !== 429
}
