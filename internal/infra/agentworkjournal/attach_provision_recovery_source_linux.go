package agentworkjournal

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// AttachProvisionRecoverySource returns only an exact retained command journal.
// Absence or mismatch cannot establish that provisioning never ran.
func (journal *Journal) AttachProvisionRecoverySource(ctx context.Context, input attachexecution.ProvisionRecoveryInput) (*AttachProvisionRecord, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	source := input.Source
	record, err := journal.readRecord(ctx, source.Forward.ID)
	if err != nil {
		return nil, err
	}
	if record == nil || !workexecution.SameAssignment(record.Attempt, source.Forward) ||
		record.Attempt.StartedAt == nil || !record.Attempt.StartedAt.Equal(*source.Forward.StartedAt) ||
		record.AttachProvision == nil || record.AttachProvision.Authority != source.Failure.Authority ||
		!record.AttachProvision.Acknowledged || record.AttachProvision.Completion != nil {
		return nil, conflict()
	}
	checkpoint, err := record.AttachProvision.Report.CheckpointAuthority(record.Attempt, record.AttachProvision.Authority)
	if err != nil || checkpoint.Sequence != source.Failure.Checkpoint.Sequence || checkpoint.Evidence != source.Failure.Checkpoint.Evidence ||
		checkpoint.StageID != source.Failure.Checkpoint.StageID || !checkpoint.RecordedAt.Equal(source.Failure.Checkpoint.RecordedAt) {
		return nil, conflict()
	}
	if failure := record.AttachProvisionFailure; failure != nil && failure.Receipt != nil {
		raw, _, err := attachexecution.EncodeProvisionFailure(source.Forward, source.Failure)
		if err != nil || !bytes.Equal(raw, failure.Evidence) || failure.Receipt.Result == nil || *failure.Receipt.Result != source.Evidence {
			return nil, conflict()
		}
	}
	return record.AttachProvision, nil
}
