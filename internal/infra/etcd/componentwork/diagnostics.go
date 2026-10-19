package componentwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// DiagnosticBegin reads the acknowledged start belonging to this Confirm.
// Its timestamp is not reconstructed from receipt time or another attempt.
func (repository *InputRepository) DiagnosticBegin(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.ComponentWorkInput, progress componentexecution.Progress,
) (componentexecution.Progress, error) {
	if progress.Validate(view.Attempt.Attempt, input) != nil || progress.Sequence == 0 {
		return componentexecution.Progress{}, conflict("Component diagnostic has no recorded execution boundary")
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
		return componentexecution.Progress{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return componentexecution.Progress{}, conflict("Component diagnostic start observation is unavailable")
	}
	return componentexecution.DecodeProgress(view.Attempt.Attempt, input, read.Values[0].Value)
}
