import type { HealthState } from "@/lib/types";
// A backup source is an ATTACH's database, a VOLUME, or the environment's
// CONFIG (env entries: vars, files, secrets — values included, age-encrypted).
// One source per attach, never per service, so a shared attach (api + worker +
// scheduler) is backed up once, not three times. Config backs up THIS
// environment's entries only — never backing environments, never
// platform state.
export type BackupSource = {
  id: string;
  kind: "attach" | "volume" | "config";
  ref?: string; // attach id or volume name; omitted for config
  name: string;
  target: string;
};

export type BackupPolicy = {
  enabled: boolean;
  frequency: string; // Controller-evaluated bounded UTC frequency
  keep: number;
  encryption: "age" | "none";
  ageRecipientRef?: string;
  connector?: string;
  sources: BackupSource[];
  nextRun: string;
  lastRun: string;
  lastStatus: HealthState;
};

export type BackupPolicySourceRecord = {
  id: string;
  kind: BackupSource["kind"];
  targetId: string;
};

export type BackupPolicySourceInput = Pick<
  BackupPolicySourceRecord,
  "kind" | "targetId"
>;

// Authoritative Controller projection for the one Backup Policy singleton
// owned by a tenant Environment. An unconfigured policy is represented by an
// effective disabled document with optional configuration omitted and sources empty.
export type BackupPolicyDocument = {
  enabled: boolean;
  nextRunAt: string | null;
  frequency?: string;
  keep?: number;
  encryption?: "age" | "none";
  connectorId?: string;
  sources: BackupPolicySourceRecord[];
  ageRecipient?: string;
  keyEra?: number;
  keyCreatedAt?: string;
  keyRotatedAt?: string;
};

export type BackupPolicyReplacement = Omit<
  BackupPolicyDocument,
  | "sources"
  | "ageRecipient"
  | "keyEra"
  | "keyCreatedAt"
  | "keyRotatedAt"
  | "nextRunAt"
> & {
  sources: BackupPolicySourceInput[];
};

export type BackupPolicyState = {
  policy: BackupPolicyDocument;
  attaches: {
    id: string;
    name: string;
    backingProjectId: string;
    backingServiceId: string;
  }[];
  volumes: { id: string; slug: string; key: string }[];
  loaded: boolean;
  loading: boolean;
  saving: boolean;
  loadError: string | null;
  saveError: string | null;
  recoveryPoints: RecoveryPointState;
};

export type RecoveryPointState = {
  items: RecoveryPoint[];
  nextCursor: string | null;
  loaded: boolean;
  loading: boolean;
  loadingMore: boolean;
  loadError: string | null;
  failedCursor: string | null;
};

type RecoveryPointBase = {
  id: string;
  sourceId: string;
  sourceKind: BackupSource["kind"];
  targetId: string;
  createdAt: string;
  sizeBytes: number;
  status: "verified";
};

export type RecoveryPoint = RecoveryPointBase &
  ({ encrypted: true; keyEra: number } | { encrypted: false; keyEra?: never });
