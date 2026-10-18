package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// retireRecoveredVolume runs under the journal lock after validating the saved
// successful recovery receipt. Remove forward evidence before the receipt so
// an interrupted retirement can replay the same proof after a crash.
func (journal *Journal) retireRecoveredVolume(ctx context.Context, recovery VolumeRecoveryRecord) error {
	if recovery.Receipt == nil || recovery.Receipt.Outcome != workexecution.Succeeded || recovery.Report == nil ||
		!recovery.Report.BoundaryCompleted(recovery.Boundary) || recovery.Source.Validate() != nil {
		return conflict()
	}
	forward, err := journal.readRecord(ctx, recovery.Source.Forward.ID)
	if err != nil {
		return err
	}
	if forward == nil {
		return nil
	}
	if !workexecution.SameAssignment(forward.Attempt, recovery.Source.Forward) || forward.Volume == nil ||
		forward.Volume.Action != recovery.Action || forward.Volume.VolumeID != recovery.VolumeID || forward.Volume.Completion != nil {
		return conflict()
	}
	if failure := forward.Volume.Failure; failure != nil && failure.Receipt != nil {
		if *failure.Receipt.Result != recovery.Source.Evidence || !failure.Receipt.FinishedAt.Equal(*recovery.Source.Forward.FinishedAt) {
			return conflict()
		}
	}
	return journal.removeRecord(ctx, recovery.Source.Forward.ID)
}
