import { useCallback, useEffect, useRef, useState } from 'react'
import { listImages, type ImageInventory } from './api'

export function useImageInventory() {
  const [inventory, setInventory] = useState<ImageInventory | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const request = useRef<AbortController | null>(null)
  const refresh = useCallback(async () => {
    request.current?.abort()
    const current = new AbortController()
    request.current = current
    setLoading(true)
    try {
      const result = await listImages(current.signal)
      if (current.signal.aborted) return
      setInventory(result)
      setError(null)
    } catch (error) {
      if (!current.signal.aborted) setError(error instanceof Error ? error.message : 'Unable to read host images')
    } finally {
      if (!current.signal.aborted) setLoading(false)
    }
  }, [])
  useEffect(() => {
    void refresh()
    return () => request.current?.abort()
  }, [refresh])
  return { inventory, loading, error, refresh }
}
