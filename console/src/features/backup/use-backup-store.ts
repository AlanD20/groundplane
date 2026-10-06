import {
  useCallback,
  useMemo,
  useRef,
  useState,
  type MutableRefObject,
} from "react";
import type { components, operations } from "@/lib/api.generated";
import { assertOptionalBackupPolicyKeep } from "@/lib/backup-policy-contract";
import { exportBackupKey } from "./backup-key-export";
import type {
  BackupPolicyDocument,
  BackupPolicyReplacement,
  BackupPolicyState,
  RecoveryPoint,
  RecoveryPointState,
} from "@/features/backup/types";
import { newULID } from "@/lib/utils";
import { listAllVolumes, type VolumeRequest } from "@/features/volume/api";

type BackupPolicyShowResponse =
  operations["backup.policy.show"]["responses"][200]["content"]["application/json"];
type BackupPolicySetRequest =
  operations["backup.policy.set"]["requestBody"]["content"]["application/json"];
type BackupPolicySetResponse =
  operations["backup.policy.set"]["responses"][200]["content"]["application/json"];
type RecoveryPointPageResponse =
  operations["backup.points.list"]["responses"][200]["content"]["application/json"];
type RecoveryPointPageItem = NonNullable<
  RecoveryPointPageResponse["items"]
>[number];
type BackupRunTaskAccepted =
  operations["backup.run"]["responses"][202]["content"]["application/json"];
type BackupRestoreRequest =
  operations["backup.restore"]["requestBody"]["content"]["application/json"];
type BackupRestoreTaskAccepted =
  operations["backup.restore"]["responses"][202]["content"]["application/json"];
type BackupKeyRotateResponse =
  operations["backup.key.rotate"]["responses"][202]["content"]["application/json"];
type TaskAccepted = components["schemas"]["TaskAccepted"];
type AttachPageResponse =
  operations["attach.list"]["responses"][200]["content"]["application/json"];

type BackupStoreOptions = {
  active: MutableRefObject<boolean>;
  request: VolumeRequest;
  assertEnvironmentMutable: (environmentId: string, action: string) => void;
};

function backupPolicyFromAPI(
  policy: BackupPolicyShowResponse | BackupPolicySetResponse,
): BackupPolicyDocument {
  assertOptionalBackupPolicyKeep(policy.keep, "Backup policy response keep");
  if (
    policy.encryption !== undefined &&
    policy.encryption !== "age" &&
    policy.encryption !== "none"
  ) {
    throw new Error(
      `Controller returned unknown Backup Policy encryption ${policy.encryption}`,
    );
  }
  return {
    enabled: policy.enabled,
    nextRunAt: policy.next_run_at,
    frequency: policy.frequency,
    keep: policy.keep,
    encryption: policy.encryption,
    connectorId: policy.connector_id,
    sources: (policy.sources ?? []).map((source) => {
      if (
        source.kind !== "attach" &&
        source.kind !== "volume" &&
        source.kind !== "config"
      ) {
        throw new Error(
          `Controller returned unknown Backup Policy source kind ${source.kind}`,
        );
      }
      return { id: source.id, kind: source.kind, targetId: source.target_id };
    }),
    ageRecipient: policy.age_recipient,
    keyEra: policy.key_era,
    keyCreatedAt: policy.key_created_at,
    keyRotatedAt: policy.key_rotated_at,
  };
}

function backupPolicyRequest(
  input: BackupPolicyReplacement,
): BackupPolicySetRequest {
  assertOptionalBackupPolicyKeep(input.keep, "Backup policy request keep");
  return {
    enabled: input.enabled,
    frequency: input.frequency,
    keep: input.keep,
    encryption: input.encryption,
    connector_id: input.connectorId,
    sources: input.sources.map((source) => ({
      kind: source.kind,
      target_id: source.targetId,
    })),
  };
}

function emptyRecoveryPointState(): RecoveryPointState {
  return {
    items: [],
    nextCursor: null,
    loaded: false,
    loading: false,
    loadingMore: false,
    loadError: null,
    failedCursor: null,
  };
}

function emptyBackupPolicyState(): BackupPolicyState {
  return {
    policy: { enabled: false, nextRunAt: null, sources: [] },
    attaches: [],
    volumes: [],
    loaded: false,
    loading: false,
    saving: false,
    loadError: null,
    saveError: null,
    recoveryPoints: emptyRecoveryPointState(),
  };
}

function recoveryPointFromAPI(point: RecoveryPointPageItem): RecoveryPoint {
  if (point.status !== "verified")
    throw new Error(
      `Controller returned unknown Recovery Point status ${point.status}`,
    );
  if (
    !point.id ||
    !point.source_id ||
    !point.target_id ||
    Number.isNaN(Date.parse(point.created_at))
  ) {
    throw new Error("Controller returned an invalid Recovery Point identity");
  }
  if (!Number.isSafeInteger(point.size_bytes) || point.size_bytes <= 0) {
    throw new Error("Controller returned an invalid Recovery Point size");
  }
  const base = {
    capture: point.capture
      ? {
          taskId: point.capture.task_id,
          createdAt: point.capture.created_at,
          sourceCount: point.capture.source_count,
        }
      : undefined,
    id: point.id,
    connectorId: point.connector_id,
    connectorEndpoint: point.connector_endpoint,
    connectorBucket: point.connector_bucket,
    connectorPrefix: point.connector_prefix,
    sourceId: point.source_id,
    sourceKind: point.source_kind,
    targetId: point.target_id,
    createdAt: point.created_at,
    sizeBytes: point.size_bytes,
    status: "verified" as const,
  };
  if (point.encrypted) {
    const keyEra = point.key_era ?? 0;
    if (!Number.isSafeInteger(keyEra) || keyEra < 1) {
      throw new Error(
        "Controller returned an invalid encrypted Recovery Point era",
      );
    }
    return {
      ...base,
      encrypted: true,
      keyEra,
    };
  }
  if (point.key_era !== undefined)
    throw new Error(
      "Controller returned an era for an unencrypted Recovery Point",
    );
  return {
    ...base,
    encrypted: false,
  };
}

async function listBackupPolicyAttaches(
  request: VolumeRequest,
  environmentId: string,
) {
  const attaches: BackupPolicyState["attaches"] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await request<AttachPageResponse>(`/attaches?${query}`, 200);
    attaches.push(
      ...(page.items ?? []).map((attach) => ({
        id: attach.id,
        name: attach.name,
        backingProjectId: attach.backing_project_id,
        backingServiceId: attach.backing_service_id,
        credential: attach.credential,
      })),
    );
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return attaches;
}

async function listBackupPolicyVolumes(
  request: VolumeRequest,
  environmentId: string,
) {
  const volumes = await listAllVolumes(request, environmentId);
  return volumes.map((volume) => ({
    id: volume.id,
    slug: volume.slug,
    key: volume.key,
  }));
}

function requiredTaskId(
  response: { task_id?: string | null },
  operation: string,
): string {
  if (typeof response.task_id !== "string" || response.task_id.length === 0) {
    throw new Error(`Controller response is missing ${operation} task_id`);
  }
  return response.task_id;
}

export function useBackupStore({
  active,
  request,
  assertEnvironmentMutable,
}: BackupStoreOptions) {
  const [backupPolicies, setBackupPolicies] = useState<
    Record<string, BackupPolicyState>
  >({});
  const policyGenerations = useRef(new Map<string, number>());
  const policyLoads = useRef(new Map<string, Promise<void>>());
  const pointGenerations = useRef(new Map<string, number>());
  const pointLoads = useRef(new Map<string, Promise<void>>());
  const loadedPointCounts = useRef(new Map<string, number>());
  const policySaves = useRef(
    new Map<
      string,
      { fingerprint: string; promise: Promise<BackupPolicyDocument> }
    >(),
  );
  const policyReplayKeys = useRef(
    new Map<string, { fingerprint: string; key: string }>(),
  );

  const updatePolicy = useCallback(
    (environmentId: string, update: (policy: BackupPolicyState) => void) => {
      if (!active.current) return;
      setBackupPolicies((current) => {
        if (!active.current) return current;
        const policy = structuredClone(
          current[environmentId] ?? emptyBackupPolicyState(),
        );
        update(policy);
        return { ...current, [environmentId]: policy };
      });
    },
    [active],
  );

  const loadBackupPolicy = useCallback(
    async (environmentId: string) => {
      const saving = policySaves.current.get(environmentId);
      if (saving) return saving.promise.then(() => undefined);
      const current = policyLoads.current.get(environmentId);
      if (current) return current;
      const generation =
        (policyGenerations.current.get(environmentId) ?? 0) + 1;
      policyGenerations.current.set(environmentId, generation);
      updatePolicy(environmentId, (policy) => {
        // Loaded data stays usable while it is revalidated. This flag is for
        // the initial read, not every poll; consumers use it to lock editors.
        policy.loading = !policy.loaded;
        if (!policy.loaded) policy.loadError = null;
      });
      const pending = (async () => {
        try {
          const [response, attaches, volumes] = await Promise.all([
            request<BackupPolicyShowResponse>(
              `/environments/${encodeURIComponent(environmentId)}/backup-policy`,
              200,
            ),
            listBackupPolicyAttaches(request, environmentId),
            listBackupPolicyVolumes(request, environmentId),
          ]);
          if (policyGenerations.current.get(environmentId) !== generation)
            return;
          updatePolicy(environmentId, (policy) => {
            policy.policy = backupPolicyFromAPI(response);
            policy.attaches = attaches;
            policy.volumes = volumes;
            policy.loaded = true;
            policy.loading = false;
            policy.loadError = null;
            policy.saveError = null;
          });
        } catch (error) {
          if (policyGenerations.current.get(environmentId) === generation) {
            updatePolicy(environmentId, (policy) => {
              policy.loading = false;
              policy.loadError =
                error instanceof Error
                  ? error.message
                  : "Unable to load Backup Policy";
            });
          }
          throw error;
        }
      })();
      policyLoads.current.set(environmentId, pending);
      try {
        await pending;
      } finally {
        if (policyLoads.current.get(environmentId) === pending)
          policyLoads.current.delete(environmentId);
      }
    },
    [request, updatePolicy],
  );

  const loadRecoveryPoints = useCallback(
    async (environmentId: string, cursor?: string) => {
      const loadKey = `${environmentId}:${cursor ?? ""}`;
      const current = pointLoads.current.get(loadKey);
      if (current) return current;
      const generation = (pointGenerations.current.get(environmentId) ?? 0) + 1;
      pointGenerations.current.set(environmentId, generation);
      updatePolicy(environmentId, (policy) => {
        if (cursor) policy.recoveryPoints.loadingMore = true;
        else policy.recoveryPoints.loading = !policy.recoveryPoints.loaded;
        // Keep a refresh failure visible until a successful read replaces it.
        if (!policy.recoveryPoints.loaded) {
          policy.recoveryPoints.loadError = null;
          policy.recoveryPoints.failedCursor = null;
        }
      });
      const pending = (async () => {
        try {
          const query = new URLSearchParams();
          if (cursor) query.set("cursor", cursor);
          const suffix = query.size === 0 ? "" : `?${query}`;
          const response = await request<RecoveryPointPageResponse>(
            `/environments/${encodeURIComponent(environmentId)}/recovery-points${suffix}`,
            200,
          );
          if (pointGenerations.current.get(environmentId) !== generation)
            return;
          const items = (response.items ?? []).map(recoveryPointFromAPI);
          let nextCursor = response.next_cursor;
          // A refresh preserves the loaded extent, using one new fixed-revision
          // cursor chain rather than mixing old pages with a new head page.
          const loadedCount = loadedPointCounts.current.get(environmentId) ?? 0;
          while (!cursor && nextCursor && items.length < loadedCount) {
            const page = await request<RecoveryPointPageResponse>(
              `/environments/${encodeURIComponent(environmentId)}/recovery-points?${new URLSearchParams({ cursor: nextCursor })}`,
              200,
            );
            if (pointGenerations.current.get(environmentId) !== generation)
              return;
            items.push(...(page.items ?? []).map(recoveryPointFromAPI));
            nextCursor = page.next_cursor;
          }
          loadedPointCounts.current.set(
            environmentId,
            cursor ? loadedCount + items.length : items.length,
          );
          updatePolicy(environmentId, (policy) => {
            const points = policy.recoveryPoints;
            points.items = cursor ? [...points.items, ...items] : items;
            points.nextCursor = nextCursor ?? null;
            points.loaded = true;
            points.loading = false;
            points.loadingMore = false;
            points.loadError = null;
            points.failedCursor = null;
          });
        } catch (error) {
          if (pointGenerations.current.get(environmentId) === generation) {
            updatePolicy(environmentId, (policy) => {
              policy.recoveryPoints.loaded = true;
              policy.recoveryPoints.loading = false;
              policy.recoveryPoints.loadingMore = false;
              policy.recoveryPoints.loadError =
                error instanceof Error
                  ? error.message
                  : "Unable to load Recovery Points";
              policy.recoveryPoints.failedCursor = cursor ?? null;
            });
          }
          throw error;
        }
      })();
      pointLoads.current.set(loadKey, pending);
      try {
        await pending;
      } finally {
        if (pointLoads.current.get(loadKey) === pending)
          pointLoads.current.delete(loadKey);
      }
    },
    [request, updatePolicy],
  );

  const replaceBackupPolicy = useCallback(
    async (
      environmentId: string,
      input: BackupPolicyReplacement,
    ): Promise<BackupPolicyDocument> => {
      assertEnvironmentMutable(environmentId, "Backup Policy mutation");
      const body = backupPolicyRequest(input);
      const fingerprint = JSON.stringify(body);
      const inFlight = policySaves.current.get(environmentId);
      if (inFlight) {
        if (inFlight.fingerprint === fingerprint) return inFlight.promise;
        throw new Error(
          "A different Backup Policy replacement is already in progress",
        );
      }
      const priorReplay = policyReplayKeys.current.get(environmentId);
      const replay =
        priorReplay?.fingerprint === fingerprint
          ? priorReplay
          : { fingerprint, key: newULID() };
      policyReplayKeys.current.set(environmentId, replay);
      policyGenerations.current.set(
        environmentId,
        (policyGenerations.current.get(environmentId) ?? 0) + 1,
      );
      updatePolicy(environmentId, (policy) => {
        policy.saving = true;
        policy.saveError = null;
      });
      const pending = (async () => {
        try {
          const response = await request<BackupPolicySetResponse>(
            `/environments/${encodeURIComponent(environmentId)}/backup-policy`,
            200,
            { method: "PUT", body, idempotencyKey: replay.key },
          );
          const policy = backupPolicyFromAPI(response);
          if (policyReplayKeys.current.get(environmentId)?.key === replay.key) {
            policyReplayKeys.current.delete(environmentId);
          }
          updatePolicy(environmentId, (current) => {
            current.policy = policy;
            current.loaded = true;
            current.saving = false;
            current.loadError = null;
            current.saveError = null;
          });
          return policy;
        } catch (error) {
          updatePolicy(environmentId, (policy) => {
            policy.saving = false;
            policy.saveError =
              error instanceof Error
                ? error.message
                : "Unable to save Backup Policy";
          });
          throw error;
        }
      })();
      policySaves.current.set(environmentId, { fingerprint, promise: pending });
      try {
        return await pending;
      } finally {
        if (policySaves.current.get(environmentId)?.promise === pending)
          policySaves.current.delete(environmentId);
      }
    },
    [assertEnvironmentMutable, request, updatePolicy],
  );

  const runBackup = useCallback(
    async (environmentId: string): Promise<string> => {
      assertEnvironmentMutable(environmentId, "Backup run");
      const response = await request<BackupRunTaskAccepted>(
        `/environments/${encodeURIComponent(environmentId)}/backup-run`,
        202,
        { method: "POST" },
      );
      return requiredTaskId(response, "Backup run");
    },
    [assertEnvironmentMutable, request],
  );

  const restoreBackup = useCallback(
    async (
      environmentId: string,
      body: BackupRestoreRequest,
      idempotencyKey: string,
    ): Promise<string> => {
      assertEnvironmentMutable(environmentId, "Restore");
      const response = await request<BackupRestoreTaskAccepted>(
        `/environments/${encodeURIComponent(environmentId)}/restore`,
        202,
        { method: "POST", body, idempotencyKey },
      );
      return requiredTaskId(response, "Restore");
    },
    [assertEnvironmentMutable, request],
  );

  const deleteRecoveryPoint = useCallback(
    async (
      environmentId: string,
      recoveryPointId: string,
      idempotencyKey: string,
    ): Promise<string> => {
      assertEnvironmentMutable(environmentId, "Recovery Point deletion");
      const response = await request<TaskAccepted>(
        `/environments/${encodeURIComponent(environmentId)}/recovery-points/${encodeURIComponent(recoveryPointId)}`,
        202,
        { method: "DELETE", idempotencyKey },
      );
      return requiredTaskId(response, "Recovery Point deletion");
    },
    [assertEnvironmentMutable, request],
  );

  const rotateBackupKey = useCallback(
    async (environmentId: string): Promise<string> => {
      assertEnvironmentMutable(environmentId, "Backup key rotation");
      const response = await request<BackupKeyRotateResponse>(
        `/environments/${encodeURIComponent(environmentId)}/rotate-key`,
        202,
        { method: "POST" },
      );
      if (!response.task_id)
        throw new Error(
          "Controller returned an empty backup key rotation task id",
        );
      return response.task_id;
    },
    [assertEnvironmentMutable, request],
  );

  const getBackupPolicyState = useCallback(
    (environmentId: string) =>
      backupPolicies[environmentId] ?? emptyBackupPolicyState(),
    [backupPolicies],
  );

  return useMemo(
    () => ({
      backupPolicies,
      getBackupPolicyState,
      loadBackupPolicy,
      loadRecoveryPoints,
      replaceBackupPolicy,
      runBackup,
      restoreBackup,
      deleteRecoveryPoint,
      rotateBackupKey,
      exportBackupKey,
    }),
    [
      backupPolicies,
      getBackupPolicyState,
      loadBackupPolicy,
      loadRecoveryPoints,
      replaceBackupPolicy,
      runBackup,
      restoreBackup,
      deleteRecoveryPoint,
      rotateBackupKey,
    ],
  );
}
