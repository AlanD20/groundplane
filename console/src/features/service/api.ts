import type { components, operations } from '@/lib/api.generated'
import type { DeployRecord, Service } from '@/lib/types'
import { serviceObservationFromAPI } from './service-observation'

type ServiceDocument = components['schemas']['Service'] | components['schemas']['ServiceDetail']
type ReleasePageResponse = operations['release.list']['responses'][200]['content']['application/json']
type ReleasePageItem = NonNullable<ReleasePageResponse['items']>[number]

export type ServiceMutationInput = Pick<
  Service,
  'name' | 'image' | 'zones' | 'strategy' | 'onFailure' | 'healthcheck' | 'resources' | 'expose' | 'restart' | 'replicas'
>

export function serviceFromAPI(service: ServiceDocument): Service {
  if (service.runtime_intent !== 'running' && service.runtime_intent !== 'stopped' && service.runtime_intent !== 'absent') {
    throw new Error(`Controller returned unknown Service runtime intent ${service.runtime_intent}`)
  }
  const strategy = service.strategy || 'recreate'
  if (strategy !== 'blue-green' && strategy !== 'recreate' && strategy !== 'rolling') {
    throw new Error(`Controller returned unknown Service strategy ${strategy}`)
  }
  const onFailure = service.on_failure ?? 'switch_back'
  if (onFailure !== 'switch_back' && onFailure !== 'leave_active') {
    throw new Error(`Controller returned unknown Service failure policy ${onFailure}`)
  }
  const healthcheck = service.healthcheck
    ? service.healthcheck.http
      ? { kind: 'http' as const, target: service.healthcheck.http, interval: service.healthcheck.interval ?? '', timeout: service.healthcheck.timeout ?? '', startPeriod: service.healthcheck.start_period ?? '', retries: service.healthcheck.retries ?? 0 }
      : service.healthcheck.tcp
        ? { kind: 'tcp' as const, target: service.healthcheck.tcp, interval: service.healthcheck.interval ?? '', timeout: service.healthcheck.timeout ?? '', startPeriod: service.healthcheck.start_period ?? '', retries: service.healthcheck.retries ?? 0 }
        : service.healthcheck.pgrep
          ? { kind: 'pgrep' as const, target: service.healthcheck.pgrep, interval: service.healthcheck.interval ?? '', timeout: service.healthcheck.timeout ?? '', startPeriod: service.healthcheck.start_period ?? '', retries: service.healthcheck.retries ?? 0 }
          : null
    : null
  const restart = service.restart === 'always' || service.restart === 'unless-stopped' ? service.restart : 'no'
  return {
    id: service.id,
    name: service.name,
    image: service.image,
    role: service.label ?? '',
    zones: [...(service.zones ?? [])],
    strategy,
    onFailure,
    healthcheck,
    resources: { mem: service.resources?.mem ?? '', cpus: String(service.resources?.cpus ?? 0) },
    command: service.command?.join(' '),
    mounts: (service.mounts ?? []).map((mount) => mount.volume
      ? { type: 'volume' as const, volume: mount.volume, mount: mount.mount }
      : { type: 'file' as const, file: mount.file ?? '', mount: mount.mount, ro: mount.ro ?? false }),
    envFiles: [],
    environment: [],
    aliases: Object.values(service.aliases ?? {}).flatMap((values) => values ?? []),
    dependsOn: Object.keys(service.depends_on ?? {}),
    expose: [...(service.expose ?? [])],
    restart,
    replicas: service.replicas ?? 1,
    runtimeIntent: service.runtime_intent,
    observation: serviceObservationFromAPI(service.observation),
    adapter: service.adapter,
    serviceName: service.name,
    prefix: service.facts_prefix,
    nativeCompose: 'native_compose' in service ? service.native_compose : undefined,
    releaseLedger: 'release_ledger' in service
      ? (service.release_ledger.items ?? []).map((release) => releaseForServiceName(release, service.name))
      : undefined,
  }
}

export function serviceMutationBody(input: ServiceMutationInput) {
  const healthcheck = input.healthcheck ? {
    http: input.healthcheck.kind === 'http' ? input.healthcheck.target : undefined,
    tcp: input.healthcheck.kind === 'tcp' ? input.healthcheck.target : undefined,
    pgrep: input.healthcheck.kind === 'pgrep' ? input.healthcheck.target : undefined,
    interval: input.healthcheck.interval,
    timeout: input.healthcheck.timeout,
    start_period: input.healthcheck.startPeriod,
    retries: input.healthcheck.retries,
  } : {}
  return {
    image: input.image,
    zones: input.zones,
    strategy: input.strategy,
    on_failure: input.onFailure ?? 'switch_back',
    healthcheck,
    resources: { mem: input.resources.mem, cpus: Number(input.resources.cpus) },
    expose: input.expose,
    restart: input.restart,
    replicas: input.replicas,
  }
}

export function releaseFromAPI(release: ReleasePageItem, services: readonly Service[]): DeployRecord {
  return releaseForServiceName(
    release,
    services.find((service) => service.id === release.service_id)?.name ?? release.service_id,
  )
}

export function releaseForServiceName(release: ReleasePageItem, serviceName: string): DeployRecord {
  const strategy = release.strategy === 'blue-green' ? 'blue-green' : 'recreate'
  return {
    id: release.id,
    service: serviceName,
    tag: release.tag,
    digest: release.digest ?? '',
    strategy,
    when: release.completed_at ?? release.created_at,
    status: release.serving ? 'active' : 'superseded',
  }
}
