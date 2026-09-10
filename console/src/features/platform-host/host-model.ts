import type { operations } from '@/lib/api.generated'
import type { HealthState } from '@/lib/types'

export type HostResponse = operations['host.show']['responses'][200]['content']['application/json']
export type HostInfo = {
  hostname: string
  arch: string
  os: string
  uptime: string
  cpu: { model: string; cores: number; load: number }
  memory: { total: string; used: string; usedPct: number }
  disk: { total: string; used: string; usedPct: number }
  swap: { total: string; used: string; usedPct: number }
  docker: string
  etcd: { node: string; status: HealthState; dbSize: string }
  controller: { service: string; status: HealthState; version: string; update: HostResponse['controller']['update'] }
  agent: { status: HealthState; pullInterval: string; maxConcurrent: number; labels: string[] }
}

export function hostFromAPI(host: HostResponse): HostInfo {
  return {
    hostname: host.hostname, arch: host.arch, os: host.os, uptime: host.uptime,
    cpu: { model: host.cpu.model, cores: host.cpu.cores, load: host.cpu.load },
    memory: { total: host.memory.total, used: host.memory.used, usedPct: host.memory.used_pct },
    disk: { total: host.disk.total, used: host.disk.used, usedPct: host.disk.used_pct },
    swap: { total: host.swap.total, used: host.swap.used, usedPct: host.swap.used_pct },
    docker: host.docker,
    etcd: { node: host.etcd.node, status: health(host.etcd.status), dbSize: host.etcd.db_size },
    controller: {
      service: host.controller.service, status: health(host.controller.status),
      version: host.controller.version, update: host.controller.update,
    },
    agent: {
      status: health(host.agent.status), pullInterval: host.agent.pull_interval,
      maxConcurrent: host.agent.max_concurrent, labels: [...(host.agent.labels ?? [])],
    },
  }
}

function health(value: string): HealthState {
  switch (value) {
    case 'healthy': case 'degraded': case 'failed': case 'stopped': case 'pending': case 'unknown': return value
    default: throw new Error(`Controller returned unknown Host health ${value}`)
  }
}
