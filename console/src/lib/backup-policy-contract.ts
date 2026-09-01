export const MAXIMUM_BACKUP_POLICY_KEEP = Number.MAX_SAFE_INTEGER

export function isValidBackupPolicyKeep(value: number): boolean {
  return (
    Number.isSafeInteger(value) &&
    value >= 1 &&
    value <= MAXIMUM_BACKUP_POLICY_KEEP
  )
}

export function assertOptionalBackupPolicyKeep(
  value: number | undefined,
  source: string,
): void {
  if (value !== undefined && !isValidBackupPolicyKeep(value)) {
    throw new Error(
      `${source} must be an integer between 1 and ${MAXIMUM_BACKUP_POLICY_KEEP}`,
    )
  }
}
