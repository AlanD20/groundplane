import type { components } from '@/lib/api.generated'
import type { Project, Service } from '@/lib/types'

type WireServiceObservation = components['schemas']['ServiceObservation']
export type ServiceReplicaCounts = components['schemas']['ServiceReplicaCounts']
export type ServiceObservationState = WireServiceObservation['state']
export type AvailableServiceObservationState = Exclude<ServiceObservationState, 'unavailable'>

export type ServiceObservation =
  | { state: 'unavailable' }
  | {
      state: AvailableServiceObservationState
      observedAt: string
      expiresAt: string
      servingReleaseId: string
      expectedReplicas: number
      replicas: ServiceReplicaCounts
    }

const unavailableObservation: ServiceObservation = { state: 'unavailable' }

export function applyServiceObservations(projects: Project[], environmentId: string, refreshed: Service[]): void {
  const observations = new Map(refreshed.map((service) => [service.id, service.observation]))
  for (const project of projects) {
    const environment = project.environments?.find((candidate) => candidate.id === environmentId)
    if (!environment) continue
    for (const service of environment.services) {
      service.observation = observations.get(service.id) ?? { state: 'unavailable' }
    }
  }
}

function validCount(value: number): boolean {
  return Number.isInteger(value) && value >= 0
}

function observedState(expectedReplicas: number, replicas: ServiceReplicaCounts): AvailableServiceObservationState {
  const total = replicaTotal(replicas)
  if (total === 0) return 'absent'
  if (replicas.failed === total) return 'failed'
  if (replicas.stopped === total) return 'stopped'
  if (replicas.starting + replicas.transitional === total) return 'starting'
  if (total === expectedReplicas && replicas.healthy === expectedReplicas) return 'healthy'
  if (
    total === expectedReplicas &&
    replicas.healthy + replicas.running === expectedReplicas &&
    replicas.running > 0
  ) return 'running'
  return 'degraded'
}

export function serviceObservationFromAPI(observation: WireServiceObservation | undefined): ServiceObservation {
  if (!observation || observation.state === 'unavailable') return unavailableObservation

  const replicas = observation.replicas
  const observedAt = observation.observed_at
  const expiresAt = observation.expires_at
  const servingReleaseId = observation.serving_release_id
  const expectedReplicas = observation.expected_replicas
  if (
    !replicas ||
    !observedAt ||
    !expiresAt ||
    !servingReleaseId ||
    expectedReplicas === undefined ||
    !Number.isInteger(expectedReplicas) ||
    expectedReplicas < 1 ||
    !validCount(replicas.running) ||
    !validCount(replicas.healthy) ||
    !validCount(replicas.starting) ||
    !validCount(replicas.unhealthy) ||
    !validCount(replicas.transitional) ||
    !validCount(replicas.stopped) ||
    !validCount(replicas.failed)
  ) return unavailableObservation

  const observedAtMs = Date.parse(observedAt)
  const expiresAtMs = Date.parse(expiresAt)
  if (!Number.isFinite(observedAtMs) || !Number.isFinite(expiresAtMs) || expiresAtMs - observedAtMs !== 15_000) {
    return unavailableObservation
  }
  if (replicaTotal(replicas) > 4_096) return unavailableObservation
  if (observedState(expectedReplicas, replicas) !== observation.state) return unavailableObservation

  return {
    state: observation.state,
    observedAt,
    expiresAt,
    servingReleaseId,
    expectedReplicas,
    replicas: { ...replicas },
  }
}

export function currentServiceObservation(
  observation: ServiceObservation,
  now = Date.now(),
): ServiceObservation {
  if (observation.state === 'unavailable') return observation
  return Date.parse(observation.observedAt) > now || Date.parse(observation.expiresAt) <= now
    ? unavailableObservation : observation
}

export function serviceObservationState(
  observation: ServiceObservation,
  now = Date.now(),
): ServiceObservationState {
  return currentServiceObservation(observation, now).state
}

export function replicaTotal(replicas: ServiceReplicaCounts): number {
  return replicas.running + replicas.healthy + replicas.starting + replicas.unhealthy +
    replicas.transitional + replicas.stopped + replicas.failed
}

export function environmentRuntimeState(
  services: readonly { observation: ServiceObservation }[],
  now = Date.now(),
): ServiceObservationState {
  if (services.length === 0) return 'unavailable'
  const states = services.map((service) => serviceObservationState(service.observation, now))
  if (states.includes('unavailable')) return 'unavailable'
  if (states.every((state) => state === 'healthy')) return 'healthy'
  if (states.every((state) => state === 'healthy' || state === 'running')) return 'running'
  for (const state of ['absent', 'failed', 'stopped', 'starting'] as const) {
    if (states.every((candidate) => candidate === state)) return state
  }
  return 'degraded'
}

export function environmentRuntimeHint(
  services: readonly { observation: ServiceObservation }[],
  now = Date.now(),
): string {
  if (services.length === 0) return 'no Services to observe'
  const states = services.map((service) => serviceObservationState(service.observation, now))
  const unavailable = states.filter((state) => state === 'unavailable').length
  if (unavailable > 0) return `${unavailable} of ${states.length} unavailable`
  const healthy = states.filter((state) => state === 'healthy').length
  const running = states.filter((state) => state === 'running').length
  if (healthy + running === states.length) {
    return running === 0 ? 'all Services healthchecked healthy' : `${healthy} healthy · ${running} running unchecked`
  }
  const incomplete = states.length - healthy - running
  return `${incomplete} of ${states.length} incomplete runtime`
}
