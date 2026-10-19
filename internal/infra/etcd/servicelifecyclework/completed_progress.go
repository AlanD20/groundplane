package servicelifecyclework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// HasCompletedProgress uses recorded stage receipts, not sequence arithmetic:
// a retry starts a new cursor while retaining earlier completed stages.
func (repository *Repository) HasCompletedProgress(
	ctx context.Context,
	view workqueue.ResumeView,
	input *agentpb.ServiceLifecycleWorkInput,
) (bool, error) {
	checkpoint := view.Checkpoint.Record
	if checkpoint == nil {
		return false, nil
	}
	read, err := repository.read(
		ctx,
		[]string{ReportKey(view.Attempt.Attempt.ID, checkpoint.Sequence)},
		view.Work.ReadRevision,
	)
	if err != nil {
		return false, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return false, conflict("Service lifecycle checkpoint has no saved progress")
	}
	progress, err := lifecycleexecution.DecodeProgress(view.Attempt.Attempt, input, read.Values[0].Value)
	if err != nil {
		return false, err
	}
	expected, err := progress.Checkpoint(view.Attempt.Attempt, input)
	if err != nil || expected != *checkpoint {
		return false, conflict("Service lifecycle progress differs from its acknowledged checkpoint")
	}
	stages, err := lifecycleexecution.Stages(input)
	if err != nil {
		return false, err
	}
	return progress.BeganAt == nil && len(progress.Completed) == len(stages), nil
}
