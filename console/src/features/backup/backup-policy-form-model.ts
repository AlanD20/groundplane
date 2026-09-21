"use client";

import {
  MAXIMUM_BACKUP_POLICY_KEEP,
  isValidBackupPolicyKeep,
} from "@/lib/backup-policy-contract";
import type { BackupPolicyReplacement } from "@/features/backup/types";

export const BACKUP_FREQUENCY =
  /^(?:\*-\*-\*|(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) \*-\*-\*) (?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$/;

export const MAX_BACKUP_POLICY_SOURCES = 12;

export function validateBackupPolicy(
  input: BackupPolicyReplacement,
  connectorAvailable: boolean,
  sourcesAvailable: boolean,
): string | null {
  if (input.sources.length > MAX_BACKUP_POLICY_SOURCES)
    return "Select at most 12 sources.";
  const sourceKeys = input.sources.map(
    (source) => `${source.kind}:${source.targetId}`,
  );
  if (new Set(sourceKeys).size !== sourceKeys.length)
    return "Each source can be selected only once.";
  if (
    input.frequency !== undefined &&
    !BACKUP_FREQUENCY.test(input.frequency)
  ) {
    return "Enter a valid daily or weekly UTC frequency.";
  }
  if (input.keep !== undefined && !isValidBackupPolicyKeep(input.keep)) {
    return `Retention must be an integer between 1 and ${MAXIMUM_BACKUP_POLICY_KEEP}.`;
  }
  if (
    input.sources.some((source) => source.kind === "config") &&
    input.encryption !== "age"
  ) {
    return "Environment config contains secret values and requires age encryption.";
  }
  const configured =
    input.frequency !== undefined ||
    input.keep !== undefined ||
    input.encryption !== undefined ||
    input.connectorId !== undefined ||
    input.sources.length > 0;
  if (configured) {
    if (input.frequency === undefined)
      return "A configured policy requires a daily or weekly UTC frequency.";
    if (input.keep === undefined)
      return "A configured policy requires a positive retention count.";
    if (input.encryption === undefined)
      return "A configured policy requires an encryption choice.";
    if (input.connectorId === undefined)
      return "A configured policy requires a Connector.";
    if (input.sources.length === 0)
      return "A configured policy requires at least one source.";
  }
  if (!input.enabled) return null;
  if (!configured) return "Configure the policy before enabling backups.";
  if (!sourcesAvailable)
    return "Every selected source must still exist in this Environment.";
  if (!connectorAvailable)
    return "Select a Connector owned by this Environment.";
  return null;
}
