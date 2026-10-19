package entrywork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// DiagnosticBegin reads the acknowledged start belonging to this Confirm.
// Its timestamp is not reconstructed from receipt time or another attempt.
func (repository *InputRepository) DiagnosticBegin(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.EntryConfigurationWorkInput, progress entryexecution.Progress,
) (entryexecution.Progress, error) {
	if progress.Validate(view.Attempt.Attempt, input) != nil || progress.Sequence == 0 {
		return entryexecution.Progress{}, conflict("Entry diagnostic has no recorded execution boundary")
	}
	if progress.InFlight {
		return progress, nil
	}
	read, err := repository.read(
		ctx,
		[]string{ReportKey(view.Attempt.Attempt.ID, progress.Sequence-1)},
		view.Work.ReadRevision,
	)
	if err != nil {
		return entryexecution.Progress{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return entryexecution.Progress{}, conflict("Entry diagnostic start observation is unavailable")
	}
	return entryexecution.DecodeProgress(view.Attempt.Attempt, input, read.Values[0].Value)
}
