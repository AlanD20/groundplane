package servicelifecyclework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Use the retained begin report, including on completion-report replay. Receipt
// arrival and attempt assignment times are not the stage's execution start.
func (repository *Repository) LifecycleDiagnosticStages(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.ServiceLifecycleWorkInput, progress lifecycleexecution.Progress,
) (workexecution.Stage, workexecution.Stage, error) {
	if progress.Validate(view.Attempt.Attempt, input) != nil || progress.Sequence == 0 {
		return workexecution.Stage{}, workexecution.Stage{}, conflict(
			"Service lifecycle diagnostic progress is invalid",
		)
	}
	begin := progress
	if progress.BeganAt == nil {
		if len(progress.Completed) == progress.Inherited {
			return workexecution.Stage{}, workexecution.Stage{}, conflict(
				"Inherited lifecycle progress has no new stage",
			)
		}
		read, err := repository.read(
			ctx,
			[]string{ReportKey(view.Attempt.Attempt.ID, progress.Sequence-1)},
			view.Work.ReadRevision,
		)
		if err != nil {
			return workexecution.Stage{}, workexecution.Stage{}, err
		}
		defer keyvalue.ClearValues(read.Values)
		if read.Values[0] == nil {
			return workexecution.Stage{}, workexecution.Stage{}, conflict(
				"Service lifecycle diagnostic begin report is unavailable",
			)
		}
		begin, err = lifecycleexecution.DecodeProgress(view.Attempt.Attempt, input, read.Values[0].Value)
		if err != nil {
			return workexecution.Stage{}, workexecution.Stage{}, err
		}
	}
	return lifecycleexecution.DiagnosticStages(view.Attempt.Attempt, input, begin, progress)
}
