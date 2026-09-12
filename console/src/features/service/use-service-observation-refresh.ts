import { useCallback, useEffect, useRef, useState } from 'react'
import type { ServiceObservation } from './service-observation'

const REFRESH_INTERVAL_MS = 10_000

export function useVisibleServiceObservations({
  environmentIds,
  observations,
  refreshEnvironment,
}: {
  environmentIds: readonly string[]
  observations: readonly ServiceObservation[]
  refreshEnvironment: (environmentId: string, signal?: AbortSignal) => Promise<void>
}) {
  const environmentKey = [...new Set(environmentIds)].sort().join('\n')
  const expiryKey = observations
    .flatMap((observation) => observation.state === 'unavailable' ? [] : [observation.expiresAt])
    .sort()
    .join('\n')
  const refreshRef = useRef(refreshEnvironment)
  const refreshNowRef = useRef<() => void>(() => undefined)
  const [expiryGeneration, setExpiryGeneration] = useState(0)
  const [now, setNow] = useState(() => Date.now())
  const [refreshing, setRefreshing] = useState(false)
  const [refreshError, setRefreshError] = useState<string>()

  useEffect(() => {
    refreshRef.current = refreshEnvironment
  }, [refreshEnvironment])

  useEffect(() => {
    setNow(Date.now())
    const expiry = expiryKey
      .split('\n')
      .filter(Boolean)
      .map((value) => Date.parse(value))
      .filter((value) => Number.isFinite(value) && value > Date.now())
      .sort((left, right) => left - right)[0]
    if (expiry === undefined) return
    const timer = window.setTimeout(() => {
      setNow(Date.now())
      setExpiryGeneration((generation) => generation + 1)
    }, Math.max(0, expiry - Date.now() + 1))
    return () => window.clearTimeout(timer)
  }, [expiryKey, expiryGeneration])

  useEffect(() => {
    const ids = environmentKey.split('\n').filter(Boolean)
    if (ids.length === 0) return
    let stopped = false
    let timer: number | undefined
    let request: AbortController | undefined
    let rerunAfterRequest = false

    const schedule = () => {
      if (!stopped) timer = window.setTimeout(() => void poll(), REFRESH_INTERVAL_MS)
    }
    const poll = async () => {
      if (stopped) return
      if (request) {
        rerunAfterRequest = true
        return
      }
      if (document.visibilityState !== 'visible') {
        schedule()
        return
      }
      const currentRequest = new AbortController()
      request = currentRequest
      setRefreshing(true)
      setNow(Date.now())
      try {
        for (const environmentId of ids) {
          await refreshRef.current(environmentId, currentRequest.signal)
        }
        if (!stopped) setRefreshError(undefined)
      } catch (error) {
        if (!stopped && !currentRequest.signal.aborted) {
          setRefreshError(error instanceof Error ? error.message : 'Unable to refresh Service observations')
        }
      } finally {
        if (request === currentRequest) request = undefined
        if (!stopped) {
          setRefreshing(false)
          setNow(Date.now())
          if (rerunAfterRequest && document.visibilityState === 'visible') {
            rerunAfterRequest = false
            void poll()
          } else {
            schedule()
          }
        }
      }
    }
    const onVisibilityChange = () => {
      if (document.visibilityState !== 'visible') {
        request?.abort()
        return
      }
      if (timer !== undefined) window.clearTimeout(timer)
      timer = undefined
      void poll()
    }
    const refreshNow = () => {
      if (timer !== undefined) window.clearTimeout(timer)
      timer = undefined
      void poll()
    }

    document.addEventListener('visibilitychange', onVisibilityChange)
    refreshNowRef.current = refreshNow
    void poll()
    return () => {
      stopped = true
      request?.abort()
      if (timer !== undefined) window.clearTimeout(timer)
      document.removeEventListener('visibilitychange', onVisibilityChange)
      if (refreshNowRef.current === refreshNow) refreshNowRef.current = () => undefined
    }
  }, [environmentKey])

  const refreshNow = useCallback(() => {
    refreshNowRef.current()
  }, [])

  return { now, refreshing, refreshError, refreshNow }
}
