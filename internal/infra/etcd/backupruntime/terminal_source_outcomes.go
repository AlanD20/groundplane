package backupruntime

// TerminalRunOutcomes preserves the frozen source order and all source results
// in the immutable receipt; it does not read a newer run or select new points.
func TerminalRunOutcomes(run BackupRunRecord) []BackupTerminalSourceOutcome {
	outcomes := make([]BackupTerminalSourceOutcome, len(run.Sources))
	for index, source := range run.Sources {
		outcomes[index] = BackupTerminalSourceOutcome{
			Ordinal: source.Ordinal, SourceID: source.SourceID, Kind: source.Kind,
			TargetID: source.TargetID, RecoveryPointID: source.RecoveryPointID,
			RecoveryPointCreatedAt: source.RecoveryPointCreatedAt,
			State:                  source.State, Phase: source.Phase, Evidence: source.Evidence,
			Upload: source.Upload, Object: source.Object, FailureCode: source.FailureCode,
		}
	}
	return outcomes
}
