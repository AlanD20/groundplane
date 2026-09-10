import { useCallback, useEffect, useRef, useState } from 'react'
import { hostFromAPI, type HostInfo, type HostResponse } from '../platform-host/host-model'
import type { ControllerConfigRequest, ControllerConfigResponse, ControllerRequest } from './api'
import { useControllerUpdate } from './use-controller-update'

export function useControllerPlatform(request: ControllerRequest, rejected: (error: unknown) => boolean) {
  const [host, setHost] = useState<HostInfo | null>(null)
  const [hostLoading, setHostLoading] = useState(true)
  const [hostError, setHostError] = useState<string | null>(null)
  const [controllerConfig, setConfig] = useState<ControllerConfigResponse | null>(null)
  const [controllerConfigLoading, setConfigLoading] = useState(false)
  const [controllerConfigError, setConfigError] = useState<string | null>(null)
  const hostGeneration = useRef(0)
  const configGeneration = useRef(0)

  const refreshHost = useCallback(async (signal?: AbortSignal) => {
    const generation = ++hostGeneration.current
    setHostLoading(true)
    try {
      const value = hostFromAPI(await request<HostResponse>('/host', 200, { signal }))
      if (!signal?.aborted && generation === hostGeneration.current) {
        setHost(value)
        setHostError(null)
      }
      return value
    } catch (error) {
      if (!signal?.aborted && generation === hostGeneration.current) setHostError(message(error, 'Unable to load Host health'))
      throw error
    } finally {
      if (!signal?.aborted && generation === hostGeneration.current) setHostLoading(false)
    }
  }, [request])

  useEffect(() => {
    const controller = new AbortController()
    void refreshHost(controller.signal).catch(() => undefined)
    return () => controller.abort()
  }, [refreshHost])

  const refreshControllerConfig = useCallback(async (signal?: AbortSignal) => {
    const generation = ++configGeneration.current
    setConfigLoading(true)
    setConfigError(null)
    try {
      const config = await request<ControllerConfigResponse>('/controller/config', 200, { signal })
      if (!signal?.aborted && generation === configGeneration.current) setConfig(config)
      return config
    } catch (error) {
      if (!signal?.aborted && generation === configGeneration.current) setConfigError(message(error, 'Unable to load Controller config'))
      throw error
    } finally {
      if (!signal?.aborted && generation === configGeneration.current) setConfigLoading(false)
    }
  }, [request])

  const setControllerConfig = useCallback(async (config: ControllerConfigRequest) => {
    ++configGeneration.current
    const updated = await request<ControllerConfigResponse>('/controller/config', 200, { method: 'PUT', body: config })
    setConfig(updated)
    setConfigError(null)
    setConfigLoading(false)
    return updated
  }, [request])

  const update = useControllerUpdate(request, rejected, host, refreshHost)
  return {
    host, hostLoading, hostError, refreshHost,
    controllerConfig, controllerConfigLoading, controllerConfigError, refreshControllerConfig, setControllerConfig,
    ...update,
  }
}

function message(error: unknown, fallback: string) { return error instanceof Error ? error.message : fallback }
