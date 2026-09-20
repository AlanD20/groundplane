"use client";

import { useStore } from "@/lib/store";
import type { BackupPolicySourceRecord, Environment } from "@/lib/types";

export function deriveStrategy(
  store: ReturnType<typeof useStore>,
  env: Environment,
) {
  const kinds = Array.from(
    new Set(
      store
        .getBackupPolicyState(env.id)
        .policy.sources.map((source) =>
          backupSourceStrategy(store, env, source),
        ),
    ),
  );
  if (kinds.includes("volume")) return "per source";
  return kinds.join(" + ") || "—";
}

export function backupSourceStrategy(
  store: ReturnType<typeof useStore>,
  env: Environment,
  source: BackupPolicySourceRecord,
): "postgres:16" | "config" | "volume" | "unsupported" {
  if (source.kind !== "attach") return source.kind;
  const attach = store
    .getBackupPolicyState(env.id)
    .attaches.find((candidate) => candidate.id === source.targetId);
  const adapter = store
    .getBackingProject(attach?.backingProjectId ?? "")
    ?.environments?.[0]?.services.find(
      (service) => service.id === attach?.backingServiceId,
    )?.adapter;
  return adapter === "postgres:16" ? adapter : "unsupported";
}

export function backupSourceLabel(
  store: ReturnType<typeof useStore>,
  env: Environment,
  source: BackupPolicySourceRecord,
) {
  if (source.kind === "config") return `${env.name} config`;
  if (source.kind === "volume") {
    return (
      store
        .getBackupPolicyState(env.id)
        .volumes.find((volume) => volume.id === source.targetId)?.slug ??
      source.targetId
    );
  }
  const attach = store
    .getBackupPolicyState(env.id)
    .attaches.find((candidate) => candidate.id === source.targetId);
  const backing = attach
    ? store.getBackingProject(attach.backingProjectId)
    : undefined;
  return attach
    ? `${backing?.name ? `${backing.name} · ` : ""}${attach.name}`
    : source.targetId;
}

export function backupPolicyConfigured(
  policy: ReturnType<typeof useStore>["backupPolicies"][string]["policy"],
) {
  return Boolean(
    policy.frequency ||
    policy.keep !== undefined ||
    policy.encryption ||
    policy.connectorId ||
    policy.sources.length > 0 ||
    policy.ageRecipient,
  );
}

export function adapterLabel(kind: string) {
  if (kind === "postgres:16") return "PostgreSQL 16";
  if (kind === "config") return "Environment config";
  if (kind === "unsupported") return "Attach · unsupported for MVP backups";
  return "Volume archive";
}

export function adapterSteps(kind: string) {
  if (kind === "unsupported") return [];
  if (kind === "config")
    return [
      { op: "export", detail: "serialize env entries (vars, files, secrets)" },
      { op: "encrypt", detail: "age-encrypt the bundle" },
      { op: "upload", detail: "r2://backups/…" },
      { op: "verify", detail: "HeadObject" },
      { op: "prune", detail: "past retention" },
    ];
  if (kind === "postgres:16")
    return [
      { op: "dump", detail: "pg_dump --format=custom" },
      { op: "encrypt", detail: "age-encrypt with recipient" },
      { op: "upload", detail: "r2://backups/…" },
      { op: "verify", detail: "HeadObject" },
      { op: "prune", detail: "past retention" },
    ];
  return [
    { op: "archive", detail: "tar archive of volume dir" },
    { op: "encrypt", detail: "age-encrypt" },
    { op: "upload", detail: "r2://backups/…" },
    { op: "verify", detail: "HeadObject" },
    { op: "prune", detail: "past retention" },
  ];
}
