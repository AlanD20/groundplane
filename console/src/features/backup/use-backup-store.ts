import {
  useCallback,
  useMemo,
  useRef,
  useState,
  type MutableRefObject,
} from "react";
import type { operations } from "@/lib/api.generated";
import { assertOptionalBackupPolicyKeep } from "@/lib/backup-policy-contract";
import {
  ControllerTransportError,
  controllerResponseError,
} from "@/lib/controller-request-errors";
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
type BackupKeyRotateResponse =
  operations["backup.key.rotate"]["responses"][202]["content"]["application/json"];
type AttachPageResponse =
  operations["attach.list"]["responses"][200]["content"]["application/json"];

type BackupStoreOptions = {
  active: MutableRefObject<boolean>;
  request: VolumeRequest;
  requestKeyRotation: (
    environmentId: string,
  ) => Promise<BackupKeyRotateResponse>;
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
  if (point.encrypted) {
    const keyEra = point.key_era ?? 0;
    if (!Number.isSafeInteger(keyEra) || keyEra < 1) {
      throw new Error(
        "Controller returned an invalid encrypted Recovery Point era",
      );
    }
    return {
      id: point.id,
      sourceId: point.source_id,
      sourceKind: point.source_kind,
      targetId: point.target_id,
      createdAt: point.created_at,
      sizeBytes: point.size_bytes,
      encrypted: true,
      keyEra,
      status: "verified",
    };
  }
  if (point.key_era !== undefined)
    throw new Error(
      "Controller returned an era for an unencrypted Recovery Point",
    );
  return {
    id: point.id,
    sourceId: point.source_id,
    sourceKind: point.source_kind,
    targetId: point.target_id,
    createdAt: point.created_at,
    sizeBytes: point.size_bytes,
    encrypted: false,
    status: "verified",
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

async function exportBackupKey(
  environmentId: string,
  signal?: AbortSignal,
): Promise<void> {
  const path = `/environments/${encodeURIComponent(environmentId)}/export-key`;
  let response: globalThis.Response | null = null;
  let blob: Blob | null = null;
  let objectURL: string | null = null;
  let anchor: HTMLAnchorElement | null = null;
  try {
    try {
      response = await fetch(`/api/v1${path}`, {
        method: "POST",
        headers: { Accept: "text/plain" },
        cache: "no-store",
        signal,
      });
    } catch (error) {
      throw new ControllerTransportError(
        `POST ${path}: ${error instanceof Error ? error.message : "request failed before an HTTP response"}`,
        error,
      );
    }
    if (response.status !== 200)
      throw await controllerResponseError(response, "POST", path);
    if (response.headers.get("Cache-Control")?.toLowerCase() !== "no-store") {
      throw new Error(
        "Controller backup key export response is not marked no-store",
      );
    }
    if (
      response.headers.get("Content-Type")?.toLowerCase() !==
      "text/plain; charset=utf-8"
    ) {
      throw new Error(
        "Controller backup key export response has an invalid content type",
      );
    }
    const disposition = response.headers.get("Content-Disposition") ?? "";
    const filenamePattern = new RegExp(
      '^attachment; filename="(groundplane-' +
        environmentId +
        '-age-era-[1-9][0-9]*-identity\\.txt)"$',
    );
    const filename = disposition.match(filenamePattern)?.[1];
    if (!filename)
      throw new Error(
        "Controller backup key export response has an invalid attachment name",
      );
    blob = await response.blob();
    objectURL = URL.createObjectURL(blob);
    anchor = document.createElement("a");
    anchor.href = objectURL;
    anchor.download = filename;
    anchor.click();
  } finally {
    if (anchor) {
      anchor.removeAttribute("href");
      anchor.remove();
    }
    if (objectURL) URL.revokeObjectURL(objectURL);
    anchor = null;
    objectURL = null;
    blob = null;
    response = null;
  }
}

export function useBackupStore({
  active,
  request,
  requestKeyRotation,
  assertEnvironmentMutable,
}: BackupStoreOptions) {
  const [backupPolicies, setBackupPolicies] = useState<
    Record<string, BackupPolicyState>
  >({});
  const policyGenerations = useRef(new Map<string, number>());
  const policyLoads = useRef(new Map<string, Promise<void>>());
  const pointGenerations = useRef(new Map<string, number>());
  const pointLoads = useRef(new Map<string, Promise<void>>());
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
        policy.loading = true;
        policy.loadError = null;
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
        else policy.recoveryPoints.loading = true;
        policy.recoveryPoints.loadError = null;
        policy.recoveryPoints.failedCursor = null;
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
          updatePolicy(environmentId, (policy) => {
            const points = policy.recoveryPoints;
            points.items = cursor ? [...points.items, ...items] : items;
            points.nextCursor = response.next_cursor ?? null;
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

  const rotateBackupKey = useCallback(
    async (environmentId: string): Promise<string> => {
      assertEnvironmentMutable(environmentId, "Backup key rotation");
      const response = await requestKeyRotation(environmentId);
      if (!response.task_id)
        throw new Error(
          "Controller returned an empty backup key rotation task id",
        );
      return response.task_id;
    },
    [assertEnvironmentMutable, requestKeyRotation],
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
      rotateBackupKey,
    ],
  );
}
