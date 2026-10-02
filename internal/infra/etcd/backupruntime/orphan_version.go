package backupruntime

// Each native reconciliation rewrite touches the orphan and both indexes in
// one transaction. The three independent monotonic facts each add one version.
func backupOrphanRecordVersion(record BackupOrphanRecord) int64 {
	version := int64(1)
	if record.CleanupProof != (BackupOrphanCleanupProof{}) {
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
