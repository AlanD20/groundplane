package backupruntime

// BackupPruneDispatchRecordsEqual compares the immutable dispatch identity and
// its ordered recovery-point selection.
func BackupPruneDispatchRecordsEqual(
	left BackupRecoveryPointPruneDispatchRecord,
	right BackupRecoveryPointPruneDispatchRecord,
) bool {
	if left.TaskID != right.TaskID || left.OperationID != right.OperationID ||
		left.EnvironmentID != right.EnvironmentID || left.CreatedAt != right.CreatedAt ||
		len(left.RecoveryPointIDs) != len(right.RecoveryPointIDs) {
		return false
	}
	for index := range left.RecoveryPointIDs {
		if left.RecoveryPointIDs[index] != right.RecoveryPointIDs[index] {
			return false
		}
	}
	return true
}

// BackupPruneDispatchContains reports whether the dispatch selected a point.
func BackupPruneDispatchContains(dispatch BackupRecoveryPointPruneDispatchRecord, recoveryPointID string) bool {
	_, found := BackupPruneDispatchPointOrdinal(dispatch, recoveryPointID)
	return found
}

// BackupPruneDispatchPointOrdinal returns a point's stable dispatch ordinal.
func BackupPruneDispatchPointOrdinal(
	dispatch BackupRecoveryPointPruneDispatchRecord,
	pointID string,
) (uint32, bool) {
	for index, candidate := range dispatch.RecoveryPointIDs {
		if candidate == pointID {
			return uint32(index), true
		}
	}
	return 0, false
}
