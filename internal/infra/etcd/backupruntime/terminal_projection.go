package backupruntime

import ()

func BackupRunReadyForSuccessfulTerminal(current BackupRunRecord, next BackupRunRecord) bool {
	if len(current.Sources) != len(next.Sources) {
		return false
	}
	for index := range current.Sources {
		if current.Sources[index].State != BackupSourceAttemptSucceeded ||
			next.Sources[index].State != BackupSourceAttemptSucceeded ||
			!backupRunSourceMutableEqual(current.Sources[index], next.Sources[index]) {
			return false
		}
	}
	return true
}

func BackupRunNeedsTerminalOrphan(current BackupRunRecord, next BackupRunRecord) bool {
	ordinal, changed := ChangedBackupSourceOrdinal(current, next)
	if !changed {
		return false
	}
	from := current.Sources[ordinal]
	to := next.Sources[ordinal]
	return from.State == BackupSourceAttemptStaged &&
		(from.Phase == BackupSourcePhaseUpload ||
			from.Phase == BackupSourcePhaseHeadVerification ||
			from.Phase == BackupSourcePhasePointCommit) &&
		to.State == BackupSourceAttemptOrphaned && to.Phase == from.Phase
}

func BackupRunRetainsTerminalOrphan(current BackupRunRecord, next BackupRunRecord) bool {
	ordinal, changed := ChangedBackupSourceOrdinal(current, next)
	if !changed {
		return false
	}
	from := current.Sources[ordinal]
	to := next.Sources[ordinal]
	return from.State == BackupSourceAttemptOrphaned && to.State == from.State &&
		to.Phase == from.Phase && from.FailureCode == "" && to.FailureCode != ""
}

func BackupRunRequiresTerminalOrphan(run BackupRunRecord) bool {
	for index := range run.Sources {
		source := run.Sources[index]
		if source.State == BackupSourceAttemptStaged &&
			(source.Phase == BackupSourcePhaseUpload ||
				source.Phase == BackupSourcePhaseHeadVerification ||
				source.Phase == BackupSourcePhasePointCommit) {
			return true
		}
	}
	return false
}

func BackupOrphanRecordFromRun(run BackupRunRecord, ordinal uint32) BackupOrphanRecord {
	source := run.Sources[ordinal]
	return BackupOrphanRecord{
		Point: BackupRecoveryPointSnapshot{
			ID:              source.RecoveryPointID,
			EnvironmentID:   run.EnvironmentID,
			SourceID:        source.SourceID,
			SourceKind:      source.Kind,
			TargetID:        source.TargetID,
			ConnectorID:     run.ConnectorID,
			ConnectorPrefix: run.ConnectorPrefix,
			ObjectKey:       source.ObjectKey,
			SourceFormat:    source.Format,
			Encryption:      run.Encryption,
			KeyEra:          run.KeyEra,
			Recipient:       run.Recipient,
			SizeBytes:       source.SizeBytes,
			SHA256:          source.SHA256,
			CreatedAt:       source.RecoveryPointCreatedAt,
		},
		TaskID: run.TaskID,
		Reconciliation: BackupOrphanReconciliationAuthority{
			OperationID:    run.OperationID,
			PolicyRevision: run.PolicyRevision,
			RetentionKeep:  run.RetentionKeep,
		},
		State: BackupOrphanInspect, CreatedAt: run.UpdatedAt, UpdatedAt: run.UpdatedAt,
	}
}
