package backupruntime

// Each native reconciliation rewrite touches the orphan and both indexes in
// one transaction. Cleanup may already be proved when recovery creates an
// orphan; otherwise its later transfer contributes one additional version.
func backupOrphanRecordVersion(record BackupOrphanRecord) int64 {
	version := int64(1)
	if record.CleanupProof != (BackupOrphanCleanupProof{}) && record.CleanupProof.RecordedAt.After(record.CreatedAt) {
		version++
	}
	if record.UnknownResolvedByReconciler {
		version++
	}
	if record.State == BackupOrphanDelete {
		version++
	}
	return version
}
