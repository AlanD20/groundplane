import assert from "node:assert/strict";
import test from "node:test";
import { parseDocument } from "yaml";
import { applyFirstDeploymentAttachments } from "../features/public-ingress/first-deployment-attachment-model.ts";
import { firstDeploymentBlueprint } from "../features/public-ingress/first-deployment-model.ts";

const source = `services:
  web: { image: example/web:1 }
  worker: { image: example/worker:1 }
x-gp-attachments:
  worker-db:
    backing_project: databases
    backing_service: postgres
    service: worker
    credential: { mode: new }
x-gp-entry:
  LOG_LEVEL: { kind: env, exposure: [web, worker], source: { literal: info } }
`;

const options = [
  {
    value: "postgres-id",
    label: "PostgreSQL",
    backingProject: "databases",
    backingService: "postgres",
    urlFact: { key: "pg_URL", secret: true },
  },
  {
    value: "valkey-id",
    label: "Valkey",
    backingProject: "caches",
    backingService: "valkey",
    urlFact: { key: "valkey_URL", secret: false },
  },
];
const database = {
  id: "database-row",
  backing: "postgres-id",
  name: "web-db",
  environmentKey: "DATABASE_URL",
};
const cache = {
  id: "cache-row",
  backing: "valkey-id",
  name: "web-cache",
  environmentKey: "CACHE_URL",
};

test("guided connections publish URL facts to their consumer and preserve the current Blueprint", () => {
  const document = parseDocument(source);
  const expected = document.toJS();
  applyFirstDeploymentAttachments(document, "web", [database, cache], options);
  Object.assign(expected["x-gp-attachments"], {
    "web-db": {
      backing_project: "databases",
      backing_service: "postgres",
      service: "web",
      credential: { mode: "new" },
    },
    "web-cache": {
      backing_project: "caches",
      backing_service: "valkey",
      service: "web",
      credential: { mode: "new" },
    },
  });
  Object.assign(expected["x-gp-entry"], {
    DATABASE_URL: {
      kind: "env",
      source: { fact: { attach: "web-db", key: "pg_URL" } },
      exposure: ["web"],
      secret: true,
    },
    CACHE_URL: {
      kind: "env",
      source: { fact: { attach: "web-cache", key: "valkey_URL" } },
      exposure: ["web"],
      secret: false,
    },
  });
  assert.deepEqual(parseDocument(document.toString()).toJS(), expected);
});

test("invalid mappings leave connections and existing configuration unchanged", () => {
  const document = parseDocument(source);
  const before = document.toString();
  for (const [change, error] of [
    [{ environmentKey: "LOG_LEVEL" }, /already contains LOG_LEVEL/],
    [{ environmentKey: "DATABASE_URL" }, /already contains DATABASE_URL/],
    [{ backing: "stale-id" }, /Choose an available Backing Service/],
    [{ name: "worker-db" }, /worker-db already exists/],
  ]) {
    assert.throws(
      () =>
        applyFirstDeploymentAttachments(
          document,
          "web",
          [database, { ...cache, ...change }],
          options,
        ),
      error,
    );
    assert.equal(document.toString(), before);
  }
});

test("one review combines storage, connection-backed configuration, and a migration", () => {
  const draft = {
    service: "api",
    image: "example/api:1",
    port: "8080",
    healthPath: "/health",
    zone: "application",
    subnet: "10.57.24.0/29",
    newZone: true,
    newService: true,
    entries: [],
    volumes: [
      {
        id: "mount",
        key: "api-data",
        target: "/app/data",
        existing: false,
        readOnly: false,
      },
    ],
    attachments: [{ ...database, name: "api-db" }],
    migration: { slug: "migrate-api", script: 'test -n "$DATABASE_URL"\n' },
  };
  const result = parseDocument(
    firstDeploymentBlueprint(source, draft, options),
  ).toJS();
  assert.deepEqual(result.services.worker, { image: "example/worker:1" });
  assert.deepEqual(result.volumes, { "api-data": {} });
  assert.deepEqual(result.services.api.volumes, [
    {
      type: "volume",
      source: "api-data",
      target: "/app/data",
      read_only: false,
    },
  ]);
  assert.deepEqual(result["x-gp-entry"].DATABASE_URL, {
    kind: "env",
    exposure: ["api"],
    source: { fact: { attach: "api-db", key: "pg_URL" } },
    secret: true,
  });
  assert.deepEqual(result["x-gp-scripts"]["migrate-api"], {
    slug: "migrate-api",
    service: "api",
    when: "pre-deploy",
    script: draft.migration.script,
  });
  assert.throws(
    () =>
      firstDeploymentBlueprint(
        source,
        {
          ...draft,
          entries: [
            { key: "DATABASE_URL", source: "literal", value: "conflict" },
          ],
        },
        options,
      ),
    /already contains DATABASE_URL/,
  );
});
