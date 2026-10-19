package agentworkjournal

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// AttachRecoverySource loads the retained command identities only for a new
// captured recovery assignment on the same host. Missing local state is not
// interpreted as permission to replay or proof that the command never ran.
func (journal *Journal) AttachRecoverySource(ctx context.Context, attempt workexecution.Attempt,
	source attachexecution.RemovalRecovery,
) (*AttachRemovalRecord, error) {
	if source.ValidateFor(attempt) != nil {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	record, err := journal.readRecord(ctx, source.Forward.ID)
	if err != nil {
		return nil, err
	}
	if record == nil || !workexecution.SameAssignment(record.Attempt, source.Forward) ||
		record.Attempt.StartedAt == nil || !record.Attempt.StartedAt.Equal(*source.Forward.StartedAt) ||
		record.AttachRemoval == nil || record.AttachRemoval.Authority != source.Failure.Authority ||
		!record.AttachRemoval.Acknowledged || !record.AttachRemoval.Report.Progress.InFlight || record.AttachRemoval.Completion != nil {
		return nil, conflict()
	}
	checkpoint, err := record.AttachRemoval.Report.CheckpointAuthority(record.Attempt, record.AttachRemoval.Authority)
	if err != nil || checkpoint.Sequence != source.Failure.Checkpoint.Sequence ||
		checkpoint.Evidence != source.Failure.Checkpoint.Evidence ||
		checkpoint.StageID != source.Failure.Checkpoint.StageID ||
		!checkpoint.RecordedAt.Equal(source.Failure.Checkpoint.RecordedAt) {
		return nil, conflict()
	}
	// The Controller may have settled interruption while the old Agent was
	// offline. If a local failure receipt exists, it must match exactly.
	if failure := record.AttachFailure; failure != nil && failure.Receipt != nil {
		raw, _, err := attachexecution.EncodeRemovalFailure(source.Forward, source.Failure)
		if err != nil || !bytes.Equal(raw, failure.Evidence) || failure.Receipt.Result == nil ||
			*failure.Receipt.Result != source.Evidence {
			return nil, conflict()
		}
	}
	return record.AttachRemoval, nil
}
