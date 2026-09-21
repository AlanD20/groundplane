import type { Adapter, PlatformInfra } from "./types";

// ---- fixture ids ----
// Every referenced entity has a stable, deterministic `<component>_<ulid>`; attach ids are record keys.
// Attach names are spec keys; provisioned database and role ids use the attach id tail for uniqueness.
const ULID_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

function fixtureUlid(seed: string): string {
  let h = 0x811c9dc5;
  for (const ch of seed) h = Math.imul(h ^ ch.codePointAt(0)!, 0x01000193);
  const bytes = new Uint8Array(17);
  for (let i = 0; i < 17; i++) {
    h = Math.imul(h ^ (h >>> 13), 0x5bd1e995);
    h ^= h >>> 15;
    bytes[i] = h >>> 24;
  }
  let out = "";
  let acc = 0;
  let nbits = 0;
  let bi = 0;
  while (out.length < 26) {
    if (nbits < 5) {
      acc = (acc << 8) | bytes[bi++];
      nbits += 8;
    } else {
      out += ULID_ALPHABET[(acc >>> (nbits - 5)) & 31];
      nbits -= 5;
    }
  }
  return out.toLowerCase();
}

function fixtureId(kind: string, seed: string): string {
  return `${kind}_${fixtureUlid(`${kind}:${seed}`)}`;
}

export const adapters: Adapter[] = [
  {
    key: "postgres:16",
    label: "PostgreSQL",
    prefix: "pg16",
    urlScheme: "pgsql",
    requires: { database: true, role: true },
    envVars: [
      "pg16_URL",
      "pg16_HOST",
      "pg16_PORT",
      "pg16_DATABASE",
      "pg16_ROLE",
      "pg16_PASSWORD",
    ],
    provision: [
      { op: "create_database", detail: 'CREATE DATABASE "<db>"' },
      {
        op: "create_role",
        detail: "CREATE ROLE \"<role>\" LOGIN PASSWORD '<generated>'",
      },
      { op: "grant", detail: 'GRANT ALL ON DATABASE "<db>" TO "<role>"' },
      {
        op: "schema_privileges",
        detail: 'GRANT USAGE, CREATE ON SCHEMA public TO "<role>"',
      },
    ],
  },
  {
    key: "valkey:9",
    label: "Valkey",
    prefix: "valkey9",
    urlScheme: "redis",
    requires: { database: false, role: true },
    envVars: [
      "valkey9_URL",
      "valkey9_HOST",
      "valkey9_PORT",
      "valkey9_ROLE",
      "valkey9_PASSWORD",
    ],
    provision: [
      {
        op: "create_acl_user",
        detail: "ACL SETUSER <role> on >‹generated› ~* &* +@all -@admin",
      },
      { op: "save_acl", detail: "ACL SAVE" },
    ],
  },
  {
    key: "custom",
    label: "Custom",
    prefix: "",
    urlScheme: "",
    requires: { database: false, role: false },
    envVars: [],
    provision: [],
    custom: true,
  },
];

export const platform: PlatformInfra = {
  project: "groundplane-infra",
  components: [
    {
      id: "infra-agent",
      name: "Agent",
      kind: "agent",
      status: "healthy",
      image: "groundplane/agent",
      version: "v0.4.2",
      runtime: "container · docker socket",
      mounts: [
        "/var/run/docker.sock (rw)",
        "/var/lib/groundplane/agent (rw)",
        "/run/groundplane/controller (ro)",
        "/run/groundplane/agent.yaml (ro)",
        "/run/groundplane/agent.token (ro)",
      ],
      notes: [
        "Controller-managed container · the Agent never recreates itself",
        "pulls tasks · acks on completion · no decision authority",
      ],
    },
    {
      id: "infra-coredns",
      name: "CoreDNS",
      kind: "coredns",
      status: "healthy",
      image: "coredns/coredns",
      version: "1.11.3",
      runtime: "container · host network",
      hostNetwork: true,
      mounts: [
        "/etc/groundplane/coredns/Corefile → /etc/coredns/Corefile (ro)",
      ],
      notes: [
        "Controller-rendered Corefile · reload plugin = zero-downtime swaps",
        "invalid configs are rejected — the old instance keeps serving",
        "tailnet delegation forwards the tailnet domain to 100.100.100.100",
      ],
    },
    {
      id: "infra-controller",
      name: "Controller",
      kind: "controller",
      status: "healthy",
      image: "systemd unit",
      version: "v0.4.2",
      runtime: "groundplane-controller.service",
      mounts: ["/etc/groundplane", "/infra/vol"],
      notes: [
        "the control plane — desired state, tasks, scheduling, secret store",
        "never containerized: a systemd unit that survives docker death",
      ],
    },
  ],
  agents: [
    {
      id: fixtureId("agt", "local-agent"),
      enrollmentTaskId: fixtureId("task", "local-agent-enrollment"),
      host: "qa-workload-groundplane",
      status: "healthy",
      version: "v0.4.2",
      labels: { arch: "arm64", host: "qa-workload" },
      readyAt: "2026-08-08T09:12:00Z",
      lastReportAt: "2026-08-22T10:28:00Z",
      inFlight: 1,
    },
  ],
  controllerHistory: [
    { version: "v0.4.2", when: "2w ago", status: "completed" },
    { version: "v0.4.1", when: "1mo ago", status: "completed" },
    { version: "v0.4.0", when: "2mo ago", status: "completed" },
  ],
  dns: {
    enabled: true,
    listen: "127.0.0.1:53",
    upstream: "1.1.1.1 8.8.8.8",
    upstreamAuto: true,
    tailnetDelegation: false,
    forwarders: [
      {
        id: fixtureId("fwd", "local-example"),
        domain: "local.example",
        upstream: "10.0.0.53",
      },
    ],
    staticEntries: 2,
    corefileRev: 12,
    reloaded: "1m ago · graceful",
  },
};
