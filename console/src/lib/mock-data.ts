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
const attSampleSitePgStaging = fixtureId('att', 'sample-site-pg-staging')
const attSampleSitePgProduction = fixtureId('att', 'sample-site-pg-production')

// The attach NAME is the spec key (operator decision, unique per
// environment) — nothing more. The provisioned database/role identifiers
// are RANDOM: they live on a SHARED instance serving every tenant, so
// only the attach id's random tail (chars 14-20, never the timestamp
// portion) keeps them instance-unique: <service>_<first-6-of-random-tail>.
const dbName = (service: string, attId: string) => `${service}_${attId.slice(14, 20)}`
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
    consumers: [],
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
// ---- sample-site environments ----
function sampleSiteServiceId(name: string, env: 'staging' | 'production'): string {
  return fixtureId('svc', `${name}-${env}`)
}

function sampleSiteServices(env: 'staging' | 'production'): Service[] {
  return [
    {
      id: sampleSiteServiceId('cms', env),
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
      id: sampleSiteServiceId('web', env),
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
      id: sampleSiteServiceId('sample-site-router', env),
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
        serviceId: sampleSiteServiceId('cms', env),
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
      ? [{ id: fixtureId('rte', 'sample-site-prod'), environmentId: envId, host: 'sample-site.example.com', path: '/admin*', exposure: 'public', targetServiceId: sampleSiteServiceId('cms', env), targetPort: 3000, status: 'unserved' }]
      : [{ id: fixtureId('rte', 'sample-site-staging'), environmentId: envId, host: 'staging.sample-site.example.com', path: '/', exposure: 'public', targetServiceId: sampleSiteServiceId('web', env), targetPort: 8080, status: 'served' }],
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
      { id: fixtureId('scr', `sample-site-migrate-${env}`), environmentId: envId, slug: 'migrate', serviceId: sampleSiteServiceId('cms', env), service: 'cms', when: 'pre-deploy', body: 'node ./bin/migrate.js', origin: 'blueprint', reconciliationKey: 'migrate', activeGeneration: 1, order: 0, execution: { mode: 'inherited' } },
      { id: fixtureId('scr', `sample-site-seed-${env}`), environmentId: envId, slug: 'seed-content', serviceId: sampleSiteServiceId('cms', env), service: 'cms', when: 'manual', body: 'node ./bin/seed.js --content', origin: 'api', activeGeneration: 1, order: 0, execution: { mode: 'inherited' } },
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
  { id: fixtureId('act', 'a4'), type: 'attach', title: 'Attached shared-postgres', target: 'sample-tenant-b/sample-site/production', workspace: 'sample-tenant-b', status: 'completed', actor: 'operator', ts: '1d ago', note: `provisioned database + role ${cmsDbStaging}`, taskState: 'acked' },
  { id: fixtureId('act', 'a7'), type: 'provision', title: 'Provisioned database + role', target: cmsDbStaging, workspace: 'platform', status: 'completed', actor: 'controller', ts: '1d ago', note: 'adapter postgres:16 · create_database → create_role → grants', taskState: 'acked' },
]

// The Controller owns the local Agent record and container lifecycle. The
// Agent executes assigned work and never recreates itself.

export const taskSamples: ActivityEntry[] = [
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
