import type {
  Adapter,
  ActivityEntry,
  Connector,
  Environment,
  PlatformInfra,
  Project,
  Runner,
  Service,
  Tenant,
  Zone,
} from './types'
import { createEnvironmentComponents } from './components'
// ---- fixture ids ----
// Every referenced entity has a stable, deterministic `<component>_<ulid>`; attach ids are record keys.
// Attach names are spec keys; provisioned database and role ids use the attach id tail for uniqueness.
const ULID_ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

function fixtureUlid(seed: string): string {
  let h = 0x811c9dc5
  for (const ch of seed) h = Math.imul(h ^ ch.codePointAt(0)!, 0x01000193)
  const bytes = new Uint8Array(17)
  for (let i = 0; i < 17; i++) {
    h = Math.imul(h ^ (h >>> 13), 0x5bd1e995)
    h ^= h >>> 15
    bytes[i] = h >>> 24
  }
  let out = ''
  let acc = 0
  let nbits = 0
  let bi = 0
  while (out.length < 26) {
    if (nbits < 5) {
      acc = (acc << 8) | bytes[bi++]
      nbits += 8
    } else {
      out += ULID_ALPHABET[(acc >>> (nbits - 5)) & 31]
      nbits -= 5
    }
  }
  return out.toLowerCase()
}

function fixtureId(kind: string, seed: string): string {
  return `${kind}_${fixtureUlid(`${kind}:${seed}`)}`
}

// Attach ids — record keys only; names are the spec keys.
const attPgAppStaging = fixtureId('att', 'pg-app-staging')
const attPgAppProduction = fixtureId('att', 'pg-app-production')
const attPgIdentityStaging = fixtureId('att', 'pg-identity-staging')
const attPgIdentityProduction = fixtureId('att', 'pg-identity-production')
const attSampleSitePgStaging = fixtureId('att', 'sample-site-pg-staging')
const attSampleSitePgProduction = fixtureId('att', 'sample-site-pg-production')
const attVkStaging = fixtureId('att', 'vk-staging')
const attVkProduction = fixtureId('att', 'vk-production')

// The attach NAME is the spec key (operator decision, unique per
// environment) — nothing more. The provisioned database/role identifiers
// are RANDOM: they live on a SHARED instance serving every tenant, so
// only the attach id's random tail (chars 14-20, never the timestamp
// portion) keeps them instance-unique: <service>_<first-6-of-random-tail>.
const dbName = (service: string, attId: string) => `${service}_${attId.slice(14, 20)}`
const apiDbStaging = dbName('api', attPgAppStaging)
const apiDbProduction = dbName('api', attPgAppProduction)
const identityDbStaging = dbName('identity', attPgIdentityStaging)
const identityDbProduction = dbName('identity', attPgIdentityProduction)
const cmsDbStaging = dbName('cms', attSampleSitePgStaging)
const cmsDbProduction = dbName('cms', attSampleSitePgProduction)

export const adapters: Adapter[] = [
  {
    key: 'postgres:16',
    label: 'PostgreSQL',
    prefix: 'pg16',
    urlScheme: 'pgsql',
    requires: { database: true, role: true },
    envVars: ['pg16_URL', 'pg16_HOST', 'pg16_PORT', 'pg16_DATABASE', 'pg16_ROLE', 'pg16_PASSWORD'],
    provision: [
      { op: 'create_database', detail: 'CREATE DATABASE "<db>"' },
      { op: 'create_role', detail: 'CREATE ROLE "<role>" LOGIN PASSWORD \'<generated>\'' },
      { op: 'grant', detail: 'GRANT ALL ON DATABASE "<db>" TO "<role>"' },
      { op: 'schema_privileges', detail: 'GRANT USAGE, CREATE ON SCHEMA public TO "<role>"' },
    ],
  },
  {
    key: 'valkey:9',
    label: 'Valkey',
    prefix: 'valkey9',
    urlScheme: 'redis',
    requires: { database: false, role: true },
    envVars: ['valkey9_URL', 'valkey9_HOST', 'valkey9_PORT', 'valkey9_ROLE', 'valkey9_PASSWORD'],
    provision: [
      { op: 'create_acl_user', detail: 'ACL SETUSER <role> on >‹generated› ~* &* +@all -@admin' },
      { op: 'save_acl', detail: 'ACL SAVE' },
    ],
  },
  {
    key: 'manual',
    label: 'Manual',
    prefix: '',
    urlScheme: '',
    requires: { database: false, role: false },
    envVars: [],
    provision: [],
    manual: true,
  },
]

export const tenants: Tenant[] = [
  {
    id: 'tnt_01h4x9k2m1a3c5e7g9j',
    slug: 'acme',
		name: 'Acme',
		description: 'Storefront microservice platform — Laravel API, workers, and the private identity stack.',
		deletionTaskId: null,
  },
  {
    id: 'tnt_01h5q8j2p4b6d8f0h2k',
    slug: 'sample-tenant-b',
		name: 'SampleSite Programs',
		description: 'SampleSite CMS and marketing site — a static stack sharing the platform PostgreSQL.',
		deletionTaskId: null,
  },
]
// ---- shared zones referenced across environments ----
type ZoneSeed = Omit<Zone, 'id'>
function zonesFor(
  fixtureOwner: string,
  environmentId: string,
  ownerKind: Zone['ownerKind'],
  ownerId: string,
  ...zones: Array<Pick<Zone, 'name' | 'subnet' | 'internal'>>
): Zone[] {
  return zones.map((zone) => ({
    ...zone,
    id: fixtureId('net', `${fixtureOwner}-${zone.name}`),
    environmentId,
    ownerKind,
    ownerId,
  }))
}

const zBackend: Pick<ZoneSeed, 'name' | 'subnet' | 'internal'> = {
  name: 'backend',
  subnet: '10.200.20.0/24',
  internal: true,
}
const zFrontend: Pick<ZoneSeed, 'name' | 'subnet' | 'internal'> = {
  name: 'frontend',
  subnet: '10.200.10.0/24',
  internal: false,
}
const zEgress: Pick<ZoneSeed, 'name' | 'subnet' | 'internal'> = {
  name: 'egress',
  subnet: '10.200.30.0/24',
  internal: false,
}
const zIdentity: Pick<ZoneSeed, 'name' | 'subnet' | 'internal'> = {
  name: 'identity-private',
  subnet: '10.200.50.0/24',
  internal: true,
}
const zOperator: Pick<ZoneSeed, 'name' | 'subnet' | 'internal'> = {
  name: 'operator',
  subnet: '10.200.60.0/24',
  internal: true,
}
const zSampleSite: Pick<ZoneSeed, 'name' | 'subnet' | 'internal'> = {
  name: 'sample-site',
  subnet: '10.200.40.0/24',
  internal: false,
}
// ---- backing services ----
// A backing service follows the SAME hierarchy as tenant projects:
// project -> environment -> service. A backing project holds exactly one
// environment ("main") with one adapter-backed service (the datastore);
// everything else (volumes, age key, backup policy, recovery points) lives
// on the environment like any other. Attachment opens for tenant services
// only once the backing service is running.
export const backingProjects: Project[] = [
  {
    id: 'prj_01h4x9k2m1d6f8h0j2m',
    slug: 'shared-postgres',
    name: 'Platform PostgreSQL',
    kind: 'backing',
		tenantId: null,
		description: 'Backing service: PostgreSQL 16. Built once; every environment that attaches gets its own database and role.',
		deletionTaskId: null,
    status: 'healthy',
    createdAt: '2026-05-02',
    consumers: [
      {
        tenant: 'acme',
        project: 'storefront',
        environment: 'staging',
        service: 'app-api',
        attachId: attPgAppStaging,
        database: apiDbStaging,
        role: apiDbStaging,
      },
      {
        tenant: 'acme',
        project: 'storefront',
        environment: 'staging',
        service: 'identity-intake',
        attachId: attPgIdentityStaging,
        database: identityDbStaging,
        role: identityDbStaging,
      },
      {
        tenant: 'acme',
        project: 'storefront',
        environment: 'production',
        service: 'app-api',
        attachId: attPgAppProduction,
        database: apiDbProduction,
        role: apiDbProduction,
      },
      {
        tenant: 'acme',
        project: 'storefront',
        environment: 'production',
        service: 'identity-intake',
        attachId: attPgIdentityProduction,
        database: identityDbProduction,
        role: identityDbProduction,
      },
      {
        tenant: 'sample-tenant-b',
        project: 'sample-site',
        environment: 'staging',
        service: 'cms',
        attachId: attSampleSitePgStaging,
        database: cmsDbStaging,
        role: cmsDbStaging,
      },
      {
        tenant: 'sample-tenant-b',
        project: 'sample-site',
        environment: 'production',
        service: 'cms',
        attachId: attSampleSitePgProduction,
        database: cmsDbProduction,
        role: cmsDbProduction,
      },
    ],
    environments: [
      {
        id: 'env_01h4x9k2m1j3n5p8r0v',
        projectId: 'prj_01h4x9k2m1d6f8h0j2m',
        name: 'main',
        status: 'healthy',
        release: 'none',
        deploys: [],
        releaseGroups: [],
        zones: zonesFor(
          'postgres-main',
          'env_01h4x9k2m1j3n5p8r0v',
          'backing_project',
          'prj_01h4x9k2m1d6f8h0j2m',
          zBackend,
        ),
        services: [
          {
            id: fixtureId('svc', 'postgres'),
            name: 'postgres',
            // Fixture-only projection of the immutable managed release output.
            image: 'registry.invalid/groundplane/postgres16@sha256:0000000000000000000000000000000000000000000000000000000000000001',
            role: 'Shared PostgreSQL 16',
            zones: ['backend'],
            strategy: 'recreate' as const,
            healthcheck: { kind: 'tcp' as const, target: '5432', interval: '15s', timeout: '3s', startPeriod: '20s', retries: 3 },
            resources: { mem: '2g', cpus: '2.0' },
            mounts: [{ type: 'volume' as const, volume: 'data', mount: '/var/lib/postgresql/data' }],
            envFiles: [],
            environment: [],
            aliases: [],
            dependsOn: [],
            expose: ['postgres:5432'],
            restart: 'unless-stopped' as const,
            replicas: 1,
            runtimeIntent: 'running',
            observation: { state: 'unavailable' },
            adapter: 'postgres:16',
            serviceName: 'postgres',
            prefix: 'pg16',
          },
        ],
        attaches: [],
        routes: [],
        components: createEnvironmentComponents(
          'env_01h4x9k2m1j3n5p8r0v',
          fixtureId('cmp', 'postgres-caddy'),
          fixtureId('cmp', 'postgres-tunnel'),
        ),
        volumes: [{ id: fixtureId('vol', 'pg-data'), environmentId: 'env_01h4x9k2m1j3n5p8r0v', slug: 'data', key: 'data', path: '/var/lib/groundplane/vol/platform/prj_01h4x9k2m1d6f8h0j2m/env_01h4x9k2m1j3n5p8r0v/data', state: 'active' }],
        networkPool: '10.16.0.0/16',
        networkCapacity: { totalAddresses: 65536, allocatedAddresses: 256, availableAddresses: 65280, zoneCount: 1 },
        volumeDir: '/var/lib/groundplane/vol/platform/prj_01h4x9k2m1d6f8h0j2m/env_01h4x9k2m1j3n5p8r0v',
        provisioningState: 'ready',
        createTaskId: null, deletionTaskId: null,
        entries: [],
        envVars: [],
        files: [],
        scripts: [],
        retention: { inactiveSlotDays: 7, keepImages: 3 },
        lastDeployAt: 'never',
      },
    ],
  },
  {
    id: 'prj_01h5q8j2p4e8g0k2m4',
    slug: 'shared-valkey',
    name: 'Platform Valkey',
    kind: 'backing',
		tenantId: null,
		description: 'Backing service: Valkey 9 cache and queue backend on the backend zone.',
		deletionTaskId: null,
    status: 'healthy',
    createdAt: '2026-05-02',
    consumers: [
      {
        tenant: 'acme',
        project: 'storefront',
        environment: 'staging',
        service: 'app-api',
        attachId: attVkStaging,
        database: '—',
        role: apiDbStaging,
      },
      {
        tenant: 'acme',
        project: 'storefront',
        environment: 'production',
        service: 'app-api',
        attachId: attVkProduction,
        database: '—',
        role: apiDbProduction,
      },
    ],
    environments: [
      {
        id: 'env_01h5q8j2p4k5p8r0v2x',
        projectId: 'prj_01h5q8j2p4e8g0k2m4',
        name: 'main',
        status: 'healthy',
        release: 'none',
        deploys: [],
        releaseGroups: [],
        zones: zonesFor(
          'valkey-main',
          'env_01h5q8j2p4k5p8r0v2x',
          'backing_project',
          'prj_01h5q8j2p4e8g0k2m4',
          zBackend,
        ),
        services: [
          {
            id: fixtureId('svc', 'valkey'),
            name: 'valkey',
            image: 'valkey/valkey:9-alpine',
            role: 'Shared Valkey 9',
            zones: ['backend'],
            strategy: 'recreate' as const,
            healthcheck: { kind: 'tcp' as const, target: '6379', interval: '15s', timeout: '3s', startPeriod: '20s', retries: 3 },
            resources: { mem: '512m', cpus: '1.0' },
            mounts: [{ type: 'volume' as const, volume: 'data', mount: '/data' }],
            envFiles: [],
            environment: [],
            aliases: [],
            dependsOn: [],
            expose: ['valkey:6379'],
            restart: 'unless-stopped' as const,
            replicas: 1,
            runtimeIntent: 'running',
            observation: { state: 'unavailable' },
            adapter: 'valkey:9',
            authentication: 'username_password',
            serviceName: 'valkey',
            prefix: 'valkey9',
          },
        ],
        attaches: [],
        routes: [],
        components: createEnvironmentComponents(
          'env_01h5q8j2p4k5p8r0v2x',
          fixtureId('cmp', 'valkey-caddy'),
          fixtureId('cmp', 'valkey-tunnel'),
        ),
        volumes: [{ id: fixtureId('vol', 'valkey-data'), environmentId: 'env_01h5q8j2p4k5p8r0v2x', slug: 'data', key: 'data', path: '/var/lib/groundplane/vol/platform/prj_01h5q8j2p4e8g0k2m4/env_01h5q8j2p4k5p8r0v2x/data', state: 'active' }],
        networkPool: '10.17.0.0/16',
        networkCapacity: { totalAddresses: 65536, allocatedAddresses: 256, availableAddresses: 65280, zoneCount: 1 },
        volumeDir: '/var/lib/groundplane/vol/platform/prj_01h5q8j2p4e8g0k2m4/env_01h5q8j2p4k5p8r0v2x',
        provisioningState: 'ready',
        createTaskId: null, deletionTaskId: null,
        entries: [],
        envVars: [],
        files: [],
        scripts: [],
        retention: { inactiveSlotDays: 7, keepImages: 3 },
        lastDeployAt: 'never',
      },
    ],
  },
]
// ---- helpers to build storefront environments ----
function svcId(name: string, env: 'staging' | 'production'): string {
  return fixtureId('svc', `${name}-${env}`)
}

const id6 = (attId: string) => attId.slice(14, 20)

function storefrontServices(env: 'staging' | 'production'): Service[] {
  const tag = env === 'production' ? 'sha-9f3c1ad' : 'sha-4be07d2'
  const activeSlot = env === 'production' ? 'blue' : 'green'
  return [
    {
      id: svcId('app-api', env),
      name: 'app-api',
      image: `storefront-app:${tag}`,
      role: 'Stateless Laravel API (blue/green slots)',
      zones: ['frontend', 'backend'],
      strategy: 'blue-green' as const,
      healthcheck: { kind: 'http' as const, target: '/up', interval: '10s', timeout: '3s', startPeriod: '20s', retries: 3 },
      resources: { mem: '768m', cpus: '1.5' },
      mounts: [{ type: 'volume' as const, volume: 'app-data', mount: '/var/www/html/storage' }],
      envFiles: [`secrets/.env.storefront.${env}`],
      environment: [{ id: fixtureId('ev', `api-${env}`), key: 'APP_ENV', value: env }],
      aliases: [`storefront-${activeSlot}-api`],
      dependsOn: [],
      expose: ['app-api:8080'],
      restart: 'unless-stopped' as const,
      replicas: 1,
      activeSlot: activeSlot as 'blue' | 'green',
      group: 'app' as const,
    },
    {
      id: svcId('app-websocket', env),
      name: 'app-websocket',
      image: `storefront-app:${tag}`,
      role: 'WebSocket server (WebSocket)',
      zones: ['frontend', 'backend'],
      strategy: 'recreate' as const,
      healthcheck: { kind: 'http' as const, target: '/app', interval: '15s', timeout: '3s', startPeriod: '15s', retries: 3 },
      resources: { mem: '256m', cpus: '0.5' },
      command: 'php artisan websocket:start',
      mounts: [],
      envFiles: [`secrets/.env.storefront.${env}`, `secrets/.env.storefront.${env}.app-websocket`],
      environment: [],
      aliases: ['app-websocket', 'storefront-websocket'],
      dependsOn: ['app-api'],
      expose: ['websocket:8080'],
      restart: 'unless-stopped' as const,
      replicas: env === 'production' ? 2 : 1,
      group: 'app' as const,
    },
    {
      id: svcId('app-queue', env),
      name: 'app-queue',
      image: `storefront-app:${tag}`,
      role: 'Queue worker (realtime, default)',
      zones: ['backend', 'egress'],
      strategy: 'recreate' as const,
      healthcheck: { kind: 'pgrep' as const, target: 'artisan queue:work', interval: '30s', timeout: '5s', startPeriod: '10s', retries: 3 },
      resources: { mem: '384m', cpus: '0.75' },
      command: 'php artisan queue:work --queue=realtime,default',
      mounts: [{ type: 'volume' as const, volume: 'app-data', mount: '/var/www/html/storage' }],
      envFiles: [`secrets/.env.storefront.${env}`],
      environment: [],
      aliases: [],
      dependsOn: ['app-api'],
      expose: [],
      restart: 'unless-stopped' as const,
      replicas: 1,
      group: 'workers' as const,
    },
    {
      id: svcId('app-scheduler', env),
      name: 'app-scheduler',
      image: `storefront-app:${tag}`,
      role: 'Singleton scheduler (schedule:work)',
      zones: [],
      strategy: 'recreate' as const,
      healthcheck: { kind: 'pgrep' as const, target: 'schedule:work', interval: '30s', timeout: '5s', startPeriod: '10s', retries: 3 },
      resources: { mem: '256m', cpus: '0.5' },
      command: 'php artisan schedule:work',
      mounts: [],
      envFiles: [`secrets/.env.storefront.${env}`],
      environment: [],
      aliases: [],
      dependsOn: ['app-api'],
      expose: [],
      restart: 'unless-stopped' as const,
      replicas: 1,
      group: 'workers' as const,
    },
    {
      id: svcId('identity-intake', env),
      name: 'identity-intake',
      image: 'storefront-identity:sha-2ab77e0',
      role: 'Identity verification intake',
      zones: ['identity-private', 'backend'],
      strategy: 'recreate' as const,
      healthcheck: { kind: 'http' as const, target: '/health', interval: '15s', timeout: '3s', startPeriod: '15s', retries: 3 },
      resources: { mem: '384m', cpus: '0.75' },
      mounts: [
        { type: 'file' as const, file: 'config/identity-tls/ca.pem', mount: '/etc/identity/ca.pem', ro: true },
      ],
      envFiles: [`secrets/.env.storefront.${env}`, `secrets/.env.identity`],
      environment: [],
      aliases: [],
      dependsOn: ['identity-clamav'],
      expose: ['identity-intake:9000'],
      restart: 'unless-stopped' as const,
      replicas: 1,
      group: 'identity' as const,
    },
    {
      id: svcId('identity-portal', env),
      name: 'identity-portal',
      image: 'storefront-identity:sha-2ab77e0',
      role: 'Reviewer portal (operator only)',
      zones: ['identity-private', 'operator'],
      strategy: 'recreate' as const,
      healthcheck: { kind: 'http' as const, target: '/health', interval: '15s', timeout: '3s', startPeriod: '15s', retries: 3 },
      resources: { mem: '384m', cpus: '0.75' },
      mounts: [{ type: 'file' as const, file: 'config/identity-tls/portal.pem', mount: '/etc/identity/portal.pem', ro: true }],
      envFiles: [`secrets/.env.storefront.${env}`, `secrets/.env.identity`],
      environment: [],
      aliases: [],
      dependsOn: ['identity-clamav'],
      expose: ['identity-portal:8443'],
      restart: 'unless-stopped' as const,
      replicas: 1,
      group: 'identity' as const,
    },
    {
      id: svcId('identity-worker', env),
      name: 'identity-worker',
      image: 'storefront-identity:sha-2ab77e0',
      role: 'Identity processing worker',
      zones: ['identity-private', 'egress'],
      strategy: 'recreate' as const,
      healthcheck: { kind: 'pgrep' as const, target: 'identity:work', interval: '30s', timeout: '5s', startPeriod: '10s', retries: 3 },
      resources: { mem: '384m', cpus: '0.75' },
      mounts: [],
      envFiles: [`secrets/.env.storefront.${env}`, `secrets/.env.identity`],
      environment: [],
      aliases: [],
      dependsOn: ['identity-intake'],
      expose: [],
      restart: 'unless-stopped' as const,
      replicas: 1,
      group: 'identity' as const,
    },
    {
      id: svcId('identity-clamav', env),
      name: 'identity-clamav',
      image: 'clamav/clamav:1.3',
      role: 'Malware scanning for evidence uploads',
      zones: ['identity-private', 'egress'],
      strategy: 'recreate' as const,
      healthcheck: { kind: 'pgrep' as const, target: 'clamd', interval: '30s', timeout: '10s', startPeriod: '60s', retries: 5 },
      resources: { mem: '1g', cpus: '1.0' },
      mounts: [],
      envFiles: [],
      environment: [],
      aliases: [],
      dependsOn: [],
      expose: ['clamav:3310'],
      restart: 'unless-stopped' as const,
      replicas: 1,
      group: 'identity' as const,
    },
    {
      id: svcId('identity-proxy', env),
      name: 'identity-proxy',
      image: 'caddy:2-alpine',
      role: 'Internal TLS terminator for status events',
      zones: ['identity-private', 'backend'],
      strategy: 'recreate' as const,
      healthcheck: { kind: 'tcp' as const, target: '8443', interval: '15s', timeout: '3s', startPeriod: '10s', retries: 3 },
      resources: { mem: '128m', cpus: '0.25' },
      mounts: [{ type: 'file' as const, file: 'config/identity-tls/proxy.pem', mount: '/etc/caddy/proxy.pem', ro: true }],
      envFiles: [],
      environment: [],
      aliases: [],
      dependsOn: ['identity-intake'],
      expose: ['identity-proxy:8443'],
      restart: 'unless-stopped' as const,
      replicas: 1,
      group: 'identity' as const,
    },
  ].map((service) => ({ ...service, runtimeIntent: 'running' as const, observation: { state: 'unavailable' as const } }))
}

function storefrontEnv(env: 'staging' | 'production'): Environment {
  const prod = env === 'production'
  const envId = prod ? 'env_01h4x9k2m1e7g9j1l3n' : 'env_01h4x9k2m1f8h0k2m4p'
  return {
    id: envId,
    projectId: 'prj_01h4x9k2m1b4d6f8h0j',
    name: env,
    status: env === 'staging' ? 'degraded' : 'healthy',
    release: prod ? 'sha-9f3c1ad' : 'sha-4be07d2',
    previousRelease: prod ? 'sha-71bd0c4' : 'sha-1102fe9',
    deploys: [
      { id: fixtureId('dep', 'app-api-1'), service: 'app-api', tag: prod ? 'sha-9f3c1ad' : 'sha-4be07d2', digest: 'sha256:9f3c1ad74e28ba48a0f1c6d2e5b4a1f09c8d7e6f5a4b3c2d1e0f9a8b7c6d5e4f3', strategy: 'blue-green', when: prod ? '2h ago' : '35m ago', status: 'active' },
      { id: fixtureId('dep', 'app-api-2'), service: 'app-api', tag: prod ? 'sha-71bd0c4' : 'sha-1102fe9', digest: 'sha256:71bd0c4a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f', strategy: 'blue-green', when: '2d ago', status: 'superseded' },
      { id: fixtureId('dep', 'app-queue-3'), service: 'app-queue', tag: prod ? 'sha-9f3c1ad' : 'sha-4be07d2', digest: 'sha256:9f3c1ad74e28ba48a0f1c6d2e5b4a1f09c8d7e6f5a4b3c2d1e0f9a8b7c6d5e4f3', strategy: 'recreate', when: prod ? '2h ago' : '35m ago', status: 'active' },
      { id: fixtureId('dep', 'app-websocket-4'), service: 'app-websocket', tag: prod ? 'sha-9f3c1ad' : 'sha-4be07d2', digest: 'sha256:9f3c1ad74e28ba48a0f1c6d2e5b4a1f09c8d7e6f5a4b3c2d1e0f9a8b7c6d5e4f3', strategy: 'recreate', when: prod ? '2h ago' : '35m ago', status: 'active' },
      { id: fixtureId('dep', 'app-api-5'), service: 'app-api', tag: prod ? 'sha-3e8d5f2' : 'sha-c4a9e07', digest: 'sha256:3e8d5f2c9b8a7f6e5d4c3b2a1908f7e6d5c4b3a291807f6e5d4c3b2a19', strategy: 'blue-green', when: '5d ago', status: 'superseded' },
      { id: fixtureId('dep', `app-scheduler-active-${env}`), service: 'app-scheduler', tag: prod ? 'sha-9f3c1ad' : 'sha-4be07d2', digest: 'sha256:9f3c1ad74e28ba48a0f1c6d2e5b4a1f09c8d7e6f5a4b3c2d1e0f9a8b7c6d5e4f3', strategy: 'recreate', when: prod ? '2h ago' : '35m ago', status: 'active' },
      { id: fixtureId('dep', `app-queue-previous-${env}`), service: 'app-queue', tag: prod ? 'sha-71bd0c4' : 'sha-1102fe9', digest: 'sha256:71bd0c4a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f', strategy: 'recreate', when: '2d ago', status: 'superseded' },
      { id: fixtureId('dep', `app-websocket-previous-${env}`), service: 'app-websocket', tag: prod ? 'sha-71bd0c4' : 'sha-1102fe9', digest: 'sha256:71bd0c4a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f', strategy: 'recreate', when: '2d ago', status: 'superseded' },
      { id: fixtureId('dep', `app-scheduler-previous-${env}`), service: 'app-scheduler', tag: prod ? 'sha-71bd0c4' : 'sha-1102fe9', digest: 'sha256:71bd0c4a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f', strategy: 'recreate', when: '2d ago', status: 'superseded' },
    ],
    releaseGroups: [
      {
        id: fixtureId('rg', `realtime-${env}`),
        name: 'realtime',
        services: ['app-api', 'app-queue', 'app-scheduler'],
        order: ['app-api', 'app-queue', 'app-scheduler'],
        tag: prod ? 'sha-9f3c1ad' : 'sha-4be07d2',
        onFailure: 'switch_back',
      },
    ],
    zones: zonesFor(`storefront-${env}`, envId, 'environment', envId, zFrontend, zBackend, zEgress, zIdentity, zOperator),
    services: storefrontServices(env),
    attaches: [
      // App attach: one database + one role, shared by api / worker / scheduler.
      {
        id: env === 'production' ? attPgAppProduction : attPgAppStaging,
        name: 'api-db',
        backingProjectId: 'prj_01h4x9k2m1d6f8h0j2m',
        backingServiceId: fixtureId('svc', 'postgres'),
        backingEnvironmentId: 'env_01h4x9k2m1j3n5p8r0v',
        backingNetworkId: fixtureId('net', 'postgres-main'),
        serviceId: svcId('app-api', env),
        credential: { mode: 'new' },
        grantAttachIds: [],
        factSets: [{ facts: [
          { key: 'pg16_DATABASE', secret: false }, { key: 'pg16_HOST', secret: false },
          { key: 'pg16_PASSWORD', secret: true }, { key: 'pg16_PORT', secret: false },
          { key: 'pg16_ROLE', secret: false }, { key: 'pg16_URL', secret: true },
        ] }],
        projectId: 'prj_01h4x9k2m1d6f8h0j2m',
        database: env === 'production' ? apiDbProduction : apiDbStaging,
        role: env === 'production' ? apiDbProduction : apiDbStaging,
        service: 'app-api',
        status: 'healthy',
      },
      // Identity attach: its own database, plus a GRANT on the app attach's
      // database — under identity's single role (facts show both sets).
      {
        id: env === 'production' ? attPgIdentityProduction : attPgIdentityStaging,
        name: 'identity-db',
        backingProjectId: 'prj_01h4x9k2m1d6f8h0j2m',
        backingServiceId: fixtureId('svc', 'postgres'),
        backingEnvironmentId: 'env_01h4x9k2m1j3n5p8r0v',
        backingNetworkId: fixtureId('net', 'postgres-main'),
        serviceId: svcId('identity-intake', env),
        credential: { mode: 'new' },
        grantAttachIds: [env === 'production' ? attPgAppProduction : attPgAppStaging],
        factSets: [
          { facts: [
            { key: 'pg16_DATABASE', secret: false }, { key: 'pg16_HOST', secret: false },
            { key: 'pg16_PASSWORD', secret: true }, { key: 'pg16_PORT', secret: false },
            { key: 'pg16_ROLE', secret: false }, { key: 'pg16_URL', secret: true },
          ] },
          { grantAttachId: env === 'production' ? attPgAppProduction : attPgAppStaging, facts: [
            { key: 'pg16_DATABASE', secret: false }, { key: 'pg16_HOST', secret: false },
            { key: 'pg16_PASSWORD', secret: true }, { key: 'pg16_PORT', secret: false },
            { key: 'pg16_ROLE', secret: false }, { key: 'pg16_URL', secret: true },
          ] },
        ],
        projectId: 'prj_01h4x9k2m1d6f8h0j2m',
        database: env === 'production' ? identityDbProduction : identityDbStaging,
        role: env === 'production' ? identityDbProduction : identityDbStaging,
        service: 'identity-intake',
        grants: [env === 'production' ? apiDbProduction : apiDbStaging],
        status: 'healthy',
      },
      {
        id: env === 'production' ? attVkProduction : attVkStaging,
        name: 'valkey-cache',
        backingProjectId: 'prj_01h5q8j2p4e8g0k2m4',
        backingServiceId: fixtureId('svc', 'valkey'),
        backingEnvironmentId: 'env_01h5q8j2p4k5p8r0v2x',
        backingNetworkId: fixtureId('net', 'valkey-main'),
        serviceId: svcId('app-api', env),
        credential: { mode: 'new' },
        grantAttachIds: [],
        factSets: [{ facts: [
          { key: 'vk9_HOST', secret: false }, { key: 'vk9_PASSWORD', secret: true },
          { key: 'vk9_PORT', secret: false }, { key: 'vk9_ROLE', secret: false },
          { key: 'vk9_URL', secret: true },
        ] }],
        projectId: 'prj_01h5q8j2p4e8g0k2m4',
        database: '—',
        role: env === 'production' ? apiDbProduction : apiDbStaging,
        service: 'app-api',
        status: 'healthy',
      },
    ],
    routes: [
      { id: fixtureId('rte', `app-${env}`), environmentId: envId, host: prod ? 'storefront.example.com' : 'staging.storefront.example.com', path: '/app/*', exposure: 'public', targetServiceId: svcId('app-websocket', env), targetPort: 8080, status: 'served' },
      { id: fixtureId('rte', `api-${env}`), environmentId: envId, host: prod ? 'storefront.example.com' : 'staging.storefront.example.com', path: '/*', exposure: 'public', targetServiceId: svcId('app-api', env), targetPort: 8080, status: 'served' },
      { id: fixtureId('rte', `apps-${env}`), environmentId: envId, host: '', path: '/apps/*', exposure: 'internal', targetServiceId: svcId('app-websocket', env), targetPort: 8080, status: 'served' },
    ],
    components: createEnvironmentComponents(
      envId,
      fixtureId('cmp', `storefront-caddy-${env}`),
      fixtureId('cmp', `storefront-tunnel-${env}`),
      {
        caddy: {
          enabled: true,
          zoneIds: [fixtureId('net', `storefront-${env}-frontend`)],
          pinnedIPv4: prod ? '10.200.10.2' : '10.200.11.2',
          caddyfile_template: '{gp.routes}',
        },
        tunnel: {
          enabled: true,
          secret_id: '',
        },
      },
    ),
    volumes: [
      { id: fixtureId('vol', `app-data-${env}`), environmentId: envId, slug: 'app-data', key: 'app-data', path: `/var/lib/groundplane/vol/tnt_01h4x9k2m1a3c5e7g9j/prj_01h4x9k2m1b4d6f8h0j/${envId}/app-data`, state: 'active' },
      { id: fixtureId('vol', `identity-data-${env}`), environmentId: envId, slug: 'identity-data', key: 'identity-data', path: `/var/lib/groundplane/vol/tnt_01h4x9k2m1a3c5e7g9j/prj_01h4x9k2m1b4d6f8h0j/${envId}/identity-data`, state: 'active' },
    ],
    networkPool: prod ? '10.200.0.0/16' : '10.201.0.0/16',
    networkCapacity: { totalAddresses: 65536, allocatedAddresses: 512, availableAddresses: 65024, zoneCount: 2 },
    volumeDir: `/var/lib/groundplane/vol/tnt_01h4x9k2m1a3c5e7g9j/prj_01h4x9k2m1b4d6f8h0j/${envId}`,
    provisioningState: 'ready',
    createTaskId: null, deletionTaskId: null,
    entries: [],
    envVars: [
      { id: fixtureId('ev', `app-${env}`), key: 'APP_ENV', value: env },
      { id: fixtureId('ev', `log-${env}`), key: 'LOG_CHANNEL', value: 'stderr' },
      { id: fixtureId('ev', `queue-${env}`), key: 'QUEUE_CONNECTION', value: 'redis' },
    ],
    files: [
      { id: fixtureId('file', `php-${env}`), name: 'laravel-php.ini', path: 'config/php/laravel.ini', content: 'memory_limit=512M\nopcache.enable=1\nopcache.validate_timestamps=0' },
      { id: fixtureId('file', `websocket-${env}`), name: 'websocket.conf', path: 'config/websocket/websocket.conf', content: 'host=0.0.0.0\nport=8080\nallowed_origins=*', exposedTo: 'app-websocket' },
    ],
    age: {
      recipient: prod ? 'age1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq' : 'age1rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr',
      generatedAt: '2026-06-01',
      lastRotatedAt: prod ? '2026-07-15' : undefined,
      keyEra: prod ? 2 : 1,
    },
    scripts: [
      { id: fixtureId('scr', `migrate-${env}`), environmentId: envId, slug: 'migrate', serviceId: svcId('app-api', env), service: 'app-api', when: 'pre-deploy', body: 'php artisan migrate --force --isolated', origin: 'blueprint', reconciliationKey: 'migrate', activeGeneration: 1, order: 0, execution: { mode: 'inherited' } },
      { id: fixtureId('scr', `preflight-${env}`), environmentId: envId, slug: 'realtime-preflight', serviceId: svcId('app-websocket', env), service: 'app-websocket', when: 'post-deploy', body: 'php artisan realtime:preflight', origin: 'blueprint', reconciliationKey: 'realtime-preflight', activeGeneration: 1, order: 0, execution: { mode: 'inherited' } },
      { id: fixtureId('scr', `cache-${env}`), environmentId: envId, slug: 'clear-cache', serviceId: svcId('app-api', env), service: 'app-api', when: 'manual', body: 'php artisan optimize:clear\nphp artisan config:cache', origin: 'api', activeGeneration: 1, order: 0, execution: { mode: 'inherited' } },
      { id: fixtureId('scr', `onfail-${env}`), environmentId: envId, slug: 'notify-failure', serviceId: svcId('app-api', env), service: 'app-api', when: 'on-failure', body: 'php artisan groundplane:notify --channel=ops "deploy failed"', origin: 'blueprint', reconciliationKey: 'notify-failure', activeGeneration: 1, order: 0, execution: { mode: 'inherited' } },
    ],
    backup: {
      enabled: true,
      frequency: '*-*-* 03:15:00',
      keep: 7,
      encryption: 'age',
      ageRecipientRef: prod ? 'age1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq' : 'age1rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr',
      connector: 'acme-r2',
      sources: [
        { id: fixtureId('spt', `app-${env}`), kind: 'attach', ref: env === 'production' ? attPgAppProduction : attPgAppStaging, name: 'app database', target: env === 'production' ? apiDbProduction : apiDbStaging },
        { id: fixtureId('spt', `vk-${env}`), kind: 'attach', ref: env === 'production' ? attVkProduction : attVkStaging, name: 'valkey cache', target: 'valkey' },
        { id: fixtureId('spt', `vol-${env}`), kind: 'volume', ref: 'app-data', name: 'app-data volume', target: 'app-data' },
        ...(prod ? [{ id: fixtureId('spt', `cfg-${env}`), name: 'environment config', kind: 'config' as const, target: `storefront.${env}` }] : []),
      ],
      nextRun: 'Tomorrow 03:15',
      lastRun: 'Today 03:15',
      lastStatus: 'healthy',
    },
    retention: { inactiveSlotDays: 7, keepImages: 3 },
    lastDeployAt: prod ? '2h ago' : '35m ago',
  }
}
// ---- sample-site environments ----
function sampleSiteServices(env: 'staging' | 'production'): Service[] {
  return [
    {
      id: svcId('cms', env),
      name: 'cms',
      image: 'sample-site-cms:local',
      role: 'Static CMS (admin)',
      zones: ['sample-site', 'backend'],
      strategy: 'recreate' as const,
      healthcheck: { kind: 'http' as const, target: '/admin/health', interval: '15s', timeout: '3s', startPeriod: '15s', retries: 3 },
      resources: { mem: '512m', cpus: '1.0' },
      mounts: [{ type: 'volume' as const, volume: 'sample-site-media', mount: '/app/public/media' }],
      envFiles: [`secrets/.env.sample-site.${env}`],
      environment: [{ id: fixtureId('ev', `cms-${env}`), key: 'NODE_ENV', value: env === 'production' ? 'production' : 'staging' }],
      aliases: ['sample-site-cms'],
      dependsOn: [],
      expose: ['cms:3000'],
      restart: 'unless-stopped' as const,
      replicas: 1,
      group: 'web' as const,
    },
    {
      id: svcId('web', env),
      name: 'web',
      image: 'sample-site-web:local',
      role: 'Marketing site (static)',
      zones: ['sample-site'],
      strategy: 'recreate' as const,
      healthcheck: { kind: 'http' as const, target: '/', interval: '15s', timeout: '3s', startPeriod: '10s', retries: 3 },
      resources: { mem: '256m', cpus: '0.5' },
      mounts: [],
      envFiles: [`secrets/.env.sample-site.${env}`],
      environment: [],
      aliases: ['sample-site-web'],
      dependsOn: ['cms'],
      expose: ['web:8080'],
      restart: 'unless-stopped' as const,
      replicas: 1,
      group: 'web' as const,
    },
    {
      id: svcId('sample-site-router', env),
      name: 'sample-site-router',
      image: 'caddy:2-alpine',
      role: 'Path router: /admin* → cms, else → web',
      zones: ['sample-site'],
      strategy: 'recreate' as const,
      healthcheck: { kind: 'http' as const, target: '/healthz', interval: '15s', timeout: '3s', startPeriod: '10s', retries: 3 },
      resources: { mem: '128m', cpus: '0.25' },
      mounts: [],
      envFiles: [],
      environment: [],
      aliases: [],
      dependsOn: ['web', 'cms'],
      expose: ['sample-site-router:8080'],
      restart: 'unless-stopped' as const,
      replicas: 1,
      group: 'web' as const,
    },
  ].map((service) => ({ ...service, runtimeIntent: 'running' as const, observation: { state: 'unavailable' as const } }))
}

function sampleSiteEnv(env: 'staging' | 'production'): Environment {
  const prod = env === 'production'
  const envId = prod ? 'env_01h5q8j2p4g0k2m4p7r' : 'env_01h5q8j2p4h2m4p7r9t'
  return {
    id: envId,
    projectId: 'prj_01h5q8j2p4c7e9g1k3',
    name: env,
    status: 'healthy',
    release: 'local-build',
    previousRelease: 'local-build-prev',
    deploys: [
      { id: fixtureId('dep', 'cms-6'), service: 'cms', tag: 'local-build', digest: 'local digest · latest', strategy: 'recreate', when: prod ? '1d ago' : '6h ago', status: 'active' },
      { id: fixtureId('dep', 'web-7'), service: 'web', tag: 'local-build', digest: 'local digest · latest', strategy: 'recreate', when: prod ? '1d ago' : '6h ago', status: 'active' },
      { id: fixtureId('dep', 'cms-8'), service: 'cms', tag: 'local-build-prev', digest: 'local digest · prev', strategy: 'recreate', when: '8d ago', status: 'superseded' },
    ],
    releaseGroups: [],
    zones: zonesFor(`sample-site-${env}`, envId, 'environment', envId, zSampleSite, zBackend),
    services: sampleSiteServices(env),
    attaches: [
      {
        id: env === 'production' ? attSampleSitePgProduction : attSampleSitePgStaging,
        name: 'cms-db',
        backingProjectId: 'prj_01h4x9k2m1d6f8h0j2m',
        backingServiceId: fixtureId('svc', 'postgres'),
        backingEnvironmentId: 'env_01h4x9k2m1j3n5p8r0v',
        backingNetworkId: fixtureId('net', 'postgres-main'),
        serviceId: svcId('cms', env),
        credential: { mode: 'new' },
        grantAttachIds: [],
        factSets: [{ facts: [
          { key: 'pg16_DATABASE', secret: false }, { key: 'pg16_HOST', secret: false },
          { key: 'pg16_PASSWORD', secret: true }, { key: 'pg16_PORT', secret: false },
          { key: 'pg16_ROLE', secret: false }, { key: 'pg16_URL', secret: true },
        ] }],
        projectId: 'prj_01h4x9k2m1d6f8h0j2m',
        database: env === 'production' ? cmsDbProduction : cmsDbStaging,
        role: env === 'production' ? cmsDbProduction : cmsDbStaging,
        service: 'cms',
        status: 'healthy',
      },
    ],
    routes: prod
      ? [{ id: fixtureId('rte', 'sample-site-prod'), environmentId: envId, host: 'sample-site.example.com', path: '/admin*', exposure: 'public', targetServiceId: svcId('cms', env), targetPort: 3000, status: 'unserved' }]
      : [{ id: fixtureId('rte', 'sample-site-staging'), environmentId: envId, host: 'staging.sample-site.example.com', path: '/', exposure: 'public', targetServiceId: svcId('web', env), targetPort: 8080, status: 'served' }],
    components: createEnvironmentComponents(
      envId,
      fixtureId('cmp', `sample-site-caddy-${env}`),
      fixtureId('cmp', `sample-site-tunnel-${env}`),
      prod
        ? {}
        : {
            caddy: {
              enabled: true,
              zoneIds: [fixtureId('net', `sample-site-${env}-sample-site`)],
              pinnedIPv4: '10.201.11.2',
              caddyfile_template: '{gp.routes}',
            },
            tunnel: {
              enabled: true,
              secret_id: '',
            },
          },
    ),
    volumes: [{ id: fixtureId('vol', `sample-site-media-${env}`), environmentId: envId, slug: 'sample-site-media', key: 'sample-site-media', path: `/var/lib/groundplane/vol/tnt_01h5q8j2p4b6d8f0h2k/prj_01h5q8j2p4c7e9g1k3/${envId}/sample-site-media`, state: 'active' }],
    networkPool: prod ? '10.22.0.0/16' : '10.23.0.0/16',
    networkCapacity: { totalAddresses: 65536, allocatedAddresses: 512, availableAddresses: 65024, zoneCount: 2 },
    volumeDir: `/var/lib/groundplane/vol/tnt_01h5q8j2p4b6d8f0h2k/prj_01h5q8j2p4c7e9g1k3/${envId}`,
    provisioningState: 'ready',
    createTaskId: null, deletionTaskId: null,
    entries: [],
    envVars: [
      { id: fixtureId('ev', `node-${env}`), key: 'NODE_ENV', value: prod ? 'production' : 'staging' },
    ],
    files: [
      { id: fixtureId('file', `nginx-${env}`), name: 'sample-site-nginx.conf', path: 'config/sample-site/nginx.conf', content: 'server {\n  listen 80;\n  root /app/public;\n}' },
    ],
    age: {
      recipient: prod ? 'age1ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss' : 'age1tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt',
      generatedAt: '2026-06-01',
      keyEra: 1,
    },
    scripts: [
      { id: fixtureId('scr', `sample-site-migrate-${env}`), environmentId: envId, slug: 'migrate', serviceId: svcId('cms', env), service: 'cms', when: 'pre-deploy', body: 'node ./bin/migrate.js', origin: 'blueprint', reconciliationKey: 'migrate', activeGeneration: 1, order: 0, execution: { mode: 'inherited' } },
      { id: fixtureId('scr', `sample-site-seed-${env}`), environmentId: envId, slug: 'seed-content', serviceId: svcId('cms', env), service: 'cms', when: 'manual', body: 'node ./bin/seed.js --content', origin: 'api', activeGeneration: 1, order: 0, execution: { mode: 'inherited' } },
    ],
    backup: {
      // staging never backs up — demo of the backups-off state
      enabled: env === 'production',
      frequency: '*-*-* 04:15:00',
      keep: 5,
      encryption: 'age',
      ageRecipientRef: prod ? 'age1ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss' : 'age1tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt',
      connector: 'r2-backups',
      sources: [
        { id: fixtureId('spt', `sample-site-pg-${env}`), kind: 'attach', ref: env === 'production' ? attSampleSitePgProduction : attSampleSitePgStaging, name: 'cms database', target: env === 'production' ? cmsDbProduction : cmsDbStaging },
        { id: fixtureId('spt', `sample-site-vol-${env}`), kind: 'volume', ref: 'sample-site-media', name: 'sample-site-media volume', target: 'sample-site-media' },
      ],
      nextRun: 'Tomorrow 04:15',
      lastRun: 'Today 04:15',
      lastStatus: 'healthy',
    },
    retention: { inactiveSlotDays: 7, keepImages: 3 },
    lastDeployAt: prod ? '1d ago' : '6h ago',
  }
}

export const tenantProjects: Project[] = [
  {
    id: 'prj_01h4x9k2m1b4d6f8h0j',
    slug: 'storefront',
    name: 'Storefront',
    kind: 'tenant',
		tenantId: 'tnt_01h4x9k2m1a3c5e7g9j',
		description: 'Laravel microservice architecture: blue/green API, workers, WebSocket, and the private identity stack.',
		deletionTaskId: null,
    status: 'degraded',
    createdAt: '2026-05-10',
    environments: [storefrontEnv('staging'), storefrontEnv('production')],
  },
  {
    id: 'prj_01h5q8j2p4c7e9g1k3',
    slug: 'sample-site',
    name: 'SampleSite',
    kind: 'tenant',
		tenantId: 'tnt_01h5q8j2p4b6d8f0h2k',
		description: 'CMS + marketing site. Static stack, no blue/green. Production runs with ingress components off (no Caddy, no tunnel).',
		deletionTaskId: null,
    status: 'healthy',
    createdAt: '2026-06-01',
    environments: [sampleSiteEnv('staging'), sampleSiteEnv('production')],
  },
]


export const connectors: Connector[] = [
  {
    id: fixtureId('con', 'acme-r2-production'),
    name: 'acme-r2',
    kind: 's3-compatible',
    scope: 'environment',
    scopeRef: 'env_01h4x9k2m1e7g9j1l3n',
    endpoint: 'https://<account>.r2.cloudflarestorage.com',
    bucket: 'acme-backups',
    prefix: 'backups/',
    region: 'auto',
    pathStyle: false,
    credentials: { accessKey: { kind: 'ref', name: 'R2_ACCESS_KEY_ID' }, secretKey: { kind: 'ref', name: 'R2_SECRET_ACCESS_KEY' } },
  },
  {
    id: fixtureId('con', 'acme-r2-staging'),
    name: 'acme-r2',
    kind: 's3-compatible',
    scope: 'environment',
    scopeRef: 'env_01h4x9k2m1f8h0k2m4p',
    endpoint: 'https://<account>.r2.cloudflarestorage.com',
    bucket: 'acme-backups',
    prefix: 'backups/',
    region: 'auto',
    pathStyle: false,
    credentials: { accessKey: { kind: 'ref', name: 'R2_ACCESS_KEY_ID' }, secretKey: { kind: 'ref', name: 'R2_SECRET_ACCESS_KEY' } },
  },
  {
    id: fixtureId('con', 'sample-site-r2-production'),
    name: 'r2-backups',
    kind: 's3-compatible',
    scope: 'environment',
    scopeRef: 'env_01h5q8j2p4g0k2m4p7r',
    endpoint: 'https://<account>.r2.cloudflarestorage.com',
    bucket: 'sample-site-backups',
    prefix: 'production/',
    region: 'auto',
    pathStyle: false,
    credentials: { accessKey: { kind: 'ref', name: 'R2_ACCESS_KEY_ID' }, secretKey: { kind: 'ref', name: 'R2_SECRET_ACCESS_KEY' } },
  },
  {
    id: fixtureId('con', 'sample-site-r2-staging'),
    name: 'r2-backups',
    kind: 's3-compatible',
    scope: 'environment',
    scopeRef: 'env_01h5q8j2p4h2m4p7r9t',
    endpoint: 'https://<account>.r2.cloudflarestorage.com',
    bucket: 'sample-site-backups',
    prefix: 'staging/',
    region: 'auto',
    pathStyle: false,
    credentials: { accessKey: { kind: 'ref', name: 'R2_ACCESS_KEY_ID' }, secretKey: { kind: 'ref', name: 'R2_SECRET_ACCESS_KEY' } },
  },
]

export const activity: ActivityEntry[] = [
  {
    id: 'a1', type: 'deploy', title: 'Deploy app-api', target: 'acme/storefront/production', workspace: 'acme', status: 'completed', actor: 'operator', ts: '2h ago',
    note: 'blue/green release sha-9f3c1ad — healthcheck passed, router reloaded',
    taskState: 'acked',
    steps: [
      { label: 'migrate expand', state: 'done' },
      { label: 'start inactive slot', state: 'done' },
      { label: 'wait /up healthcheck', state: 'done' },
      { label: 'render + validate Caddyfile', state: 'done' },
      { label: 'reload router', state: 'done' },
      { label: 'recreate workers', state: 'done' },
      { label: 'record active slot', state: 'done' },
    ],
  },
  {
    id: 'a2', type: 'backup', title: 'Backup completed', target: 'shared-postgres', workspace: 'platform', status: 'completed', actor: 'scheduler', ts: '9h ago',
    note: 'pg_dump → age-encrypt → upload → HeadObject verified → pruned past retention',
    taskState: 'acked',
    steps: [
      { label: 'pg_dump --format=custom', state: 'done' },
      { label: 'age-encrypt dump', state: 'done' },
      { label: 'upload to r2://backups/', state: 'done' },
      { label: 'HeadObject verify', state: 'done' },
      { label: 'prune past retention', state: 'done' },
    ],
  },
  {
    id: fixtureId('task', 'failed-deploy'), operationId: fixtureId('op', 'failed-deploy'), idempotencyKey: 'console:deploy:failed-deploy', planHash: '4c753dbda3e23b76b0a63ee94759e067acbc220c401594150ceafd27c5b811d4', timeout: '120s', params: { tag: 'sha-4be07d2', strategy: 'blue-green' }, type: 'deploy', title: 'Deploy failed — healthcheck', target: 'env_01h4x9k2m1f8h0k2m4p', workspace: 'acme', status: 'failed', actor: 'operator', ts: '11h ago',
    note: 'inactive slot never reached /up; on-failure hook ran',
    taskState: 'acked',
    steps: [
      { label: 'migrate expand', state: 'done' },
      { label: 'start inactive slot', state: 'done' },
      { label: 'wait /up healthcheck', state: 'failed' },
    ],
  },
  { id: fixtureId('act', 'a4'), type: 'attach', title: 'Attached shared-postgres', target: 'sample-tenant-b/sample-site/production', workspace: 'sample-tenant-b', status: 'completed', actor: 'operator', ts: '1d ago', note: `provisioned database + role ${cmsDbStaging}`, taskState: 'acked' },
  { id: 'a5', type: 'rollback', title: 'Rolled back app-api', target: 'acme/storefront/production', workspace: 'acme', status: 'completed', actor: 'operator', ts: '2d ago', note: 'traffic switch to previous image — no migration reversal', taskState: 'acked' },
  { id: fixtureId('act', 'a5'), type: 'attach', title: 'Attached shared-postgres', target: `storefront → ${apiDbProduction}`, workspace: 'platform', status: 'completed', actor: 'operator', ts: '5d ago', note: 'provisioned database + role', taskState: 'acked' },
  { id: fixtureId('act', 'a7'), type: 'provision', title: 'Provisioned database + role', target: cmsDbStaging, workspace: 'platform', status: 'completed', actor: 'controller', ts: '1d ago', note: 'adapter postgres:16 · create_database → create_role → grants', taskState: 'acked' },
]

// The Controller owns the local Agent record and container lifecycle. The
// Agent executes assigned work and never recreates itself.

export const taskSamples: ActivityEntry[] = [
  // ---- storefront / production — the live task queue ----
  {
    id: fixtureId('act', 'task-kw-prod-deploy'), type: 'deploy', title: 'Deploy app-api → sha-3e8d5f2', target: 'env_01h4x9k2m1e7g9j1l3n', workspace: 'acme',
    status: 'running', actor: 'operator', ts: 'just now',
    note: 'blue/green release — inactive slot starting; rollback point stays sha-9f3c1ad',
    taskState: 'running',
    steps: [
      { label: 'migrate expand (pre-deploy hook)', state: 'done' },
      { label: 'start inactive slot', state: 'done' },
      { label: 'wait /up healthcheck', state: 'running' },
      { label: 'render + validate Caddyfile', state: 'pending' },
      { label: 'reload router (traffic switch)', state: 'pending' },
      { label: 'recreate workers once (never two schedulers)', state: 'pending' },
      { label: 'record active slot', state: 'pending' },
    ],
  },
  {
    id: fixtureId('act', 'task-kw-prod-backup'), type: 'backup', title: 'Backup run · app database', target: 'env_01h4x9k2m1e7g9j1l3n', workspace: 'acme',
    status: 'pending', actor: 'controller', ts: '2m ago',
    note: 'scheduled *-*-* 03:15:00 · one source per attach, shared attaches backed up once',
    taskState: 'queued',
    steps: [
      { label: 'pg_dump --format=custom of api_2d1c3f9', state: 'pending' },
      { label: 'age-encrypt', state: 'pending' },
      { label: 'upload to r2://r2-backups + HeadObject verify', state: 'pending' },
      { label: 'prune past retention (keep 7)', state: 'pending' },
    ],
  },
  {
    id: fixtureId('act', 'task-kw-prod-script'), type: 'script', title: 'Run realtime-preflight (post-deploy)', target: 'env_01h4x9k2m1e7g9j1l3n', workspace: 'acme',
    status: 'completed', actor: 'operator', ts: '2h ago',
    note: 'php artisan realtime:preflight · app-websocket',
    taskState: 'acked',
    steps: [
      { label: 'pull script task', state: 'done' },
      { label: 'exec php artisan realtime:preflight in app-websocket', state: 'done' },
      { label: 'stream output to Controller', state: 'done' },
    ],
  },
  {
    id: fixtureId('act', 'task-kw-prod-attach'), type: 'attach', title: 'Attach shared-postgres → identity-intake', target: 'env_01h4x9k2m1e7g9j1l3n', workspace: 'acme',
    status: 'completed', actor: 'operator', ts: '3d ago',
    note: 'provisioned identity_2ab77e + role · grant on api_2d1c3f9',
    taskState: 'acked',
    steps: [
      { label: 'create_database · identity_2ab77e', state: 'done' },
      { label: 'create_role · identity_2ab77e', state: 'done' },
      { label: 'grant on identity_2ab77e to identity_2ab77e', state: 'done' },
      { label: 'grant api_2d1c3f9 to identity_2ab77e', state: 'done' },
      { label: 'facts available: pg16_URL …', state: 'done' },
    ],
  },
  // ---- storefront / staging ----
  {
    id: fixtureId('act', 'task-kw-staging-rollback'), type: 'rollback', title: 'Roll back app-api → sha-4be07d2', target: 'env_01h4x9k2m1f8h0k2m4p', workspace: 'acme',
    status: 'completed', actor: 'operator', ts: '35m ago',
    note: 'traffic switch, never a cold start · migrations untouched',
    taskState: 'acked',
    steps: [
      { label: 'run pre-rollback hooks', state: 'done' },
      { label: 'start previous image in inactive slot', state: 'done' },
      { label: 'wait for healthcheck', state: 'done' },
      { label: 'reload router (traffic switch)', state: 'done' },
      { label: 'run post-rollback hooks', state: 'done' },
    ],
  },
  // ---- sample-site / production ----
  {
    id: fixtureId('act', 'task-sample-site-prod-script'), type: 'script', title: 'Run seed-content (manual)', target: 'env_01h5q8j2p4g0k2m4p7r', workspace: 'sample-tenant-b',
    status: 'running', actor: 'operator', ts: 'just now',
    note: 'node ./bin/seed.js --content · cms',
    taskState: 'pulled',
    steps: [
      { label: 'pull script task', state: 'done' },
      { label: 'exec node ./bin/seed.js --content in cms', state: 'running' },
      { label: 'stream output to Controller', state: 'pending' },
    ],
  },
  // ---- platform-infra tasks (the component's own stream) ----
  {
    id: fixtureId('act', 'task-infra-dns'), type: 'run', title: 'DNS settings saved', target: 'platform-infra', workspace: 'platform',
    status: 'completed', actor: 'operator', ts: '1m ago',
    note: 'Corefile rev 13 · reloaded gracefully (zero-downtime swap)',
    taskState: 'acked',
    steps: [
      { label: 'render Corefile', state: 'done' },
      { label: 'validate (bad edit rejected, old instance keeps serving)', state: 'done' },
      { label: 'reload plugin — graceful swap', state: 'done' },
    ],
  },
  {
    id: fixtureId('act', 'task-infra-ctrl'), type: 'run', title: 'Update controller → v0.4.3', target: 'platform-infra', workspace: 'platform',
    status: 'running', actor: 'operator', ts: 'just now',
    note: 'staged-binary swap · checksum verified · old binary kept as fallback',
    taskState: 'running',
    steps: [
      { label: 'download groundplane-controller v0.4.3 → .new', state: 'done' },
      { label: 'verify checksum + version self-check', state: 'done' },
      { label: 'systemctl restart groundplane-controller (swap on start)', state: 'running' },
      { label: 'verify healthy + etcd reachable', state: 'pending' },
    ],
  },
  {
    id: fixtureId('act', 'task-infra-agent'), type: 'run', title: 'Agent config saved', target: 'platform-infra', workspace: 'platform',
    status: 'completed', actor: 'operator', ts: '2h ago',
    note: 'pull 2s · max 3 · labels qa-workload, arm64 — served on next pull, no restart',
    taskState: 'acked',
    steps: [
      { label: 'write config to etcd', state: 'done' },
      { label: 'served on next Ready', state: 'done' },
    ],
  },
]

export const platform: PlatformInfra = {
  project: 'groundplane-infra',
  components: [
    {
      id: 'infra-agent',
      name: 'Agent',
      kind: 'agent',
      status: 'healthy',
      image: 'groundplane/agent',
      version: 'v0.4.2',
      runtime: 'container · docker socket',
      mounts: [
        '/var/run/docker.sock (rw)',
        '/var/lib/groundplane/agent (rw)',
        '/run/groundplane/controller (ro)',
        '/run/groundplane/agent.yaml (ro)',
        '/run/groundplane/agent.token (ro)',
      ],
      notes: [
        'Controller-managed container · the Agent never recreates itself',
        'pulls tasks · acks on completion · no decision authority',
      ],
    },
    {
      id: 'infra-coredns',
      name: 'CoreDNS',
      kind: 'coredns',
      status: 'healthy',
      image: 'coredns/coredns',
      version: '1.11.3',
      runtime: 'container · host network',
      hostNetwork: true,
      mounts: ['/etc/groundplane/coredns/Corefile → /etc/coredns/Corefile (ro)'],
      notes: [
        'Controller-rendered Corefile · reload plugin = zero-downtime swaps',
        'invalid configs are rejected — the old instance keeps serving',
        'tailnet delegation forwards the tailnet domain to 100.100.100.100',
      ],
    },
    {
      id: 'infra-controller',
      name: 'Controller',
      kind: 'controller',
      status: 'healthy',
      image: 'systemd unit',
      version: 'v0.4.2',
      runtime: 'groundplane-controller.service',
      mounts: ['/etc/groundplane', '/infra/vol'],
      notes: [
         'the control plane — desired state, tasks, scheduling, secret store',
         'never containerized: a systemd unit that survives docker death',
       ],
    },
  ],
  agents: [
    {
      id: fixtureId('agt', 'local-agent'),
      enrollmentTaskId: fixtureId('task', 'local-agent-enrollment'),
      host: 'qa-workload-groundplane',
      status: 'healthy',
      version: 'v0.4.2',
      labels: { arch: 'arm64', host: 'qa-workload' },
      readyAt: '2026-08-08T09:12:00Z',
      lastReportAt: '2026-08-22T10:28:00Z',
      inFlight: 1,
    },
  ],
  controllerHistory: [
    { version: 'v0.4.2', when: '2w ago', status: 'completed' },
    { version: 'v0.4.1', when: '1mo ago', status: 'completed' },
    { version: 'v0.4.0', when: '2mo ago', status: 'completed' },
  ],
  dns: {
    enabled: true,
    listen: '127.0.0.1:53',
    upstream: '1.1.1.1 8.8.8.8',
    upstreamAuto: true,
    tailnetDelegation: false,
    forwarders: [{ id: fixtureId('fwd', 'local-example'), domain: 'local.example', upstream: '10.0.0.53' }],
    staticEntries: 2,
    corefileRev: 12,
    reloaded: '1m ago · graceful',
  },
}
