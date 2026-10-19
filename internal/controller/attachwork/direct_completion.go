package attachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type directCompletion struct {
	inputs *attachprovision.InputRepository
}

func DirectCompletions(inputs *attachprovision.InputRepository) ([]workdispatch.CompletionRegistration, error) {
	if inputs == nil {
		return nil, errs.New(errs.KindInternal, "Attach completion requires captured provisioning storage")
	}
	return []workdispatch.CompletionRegistration{{Action: workexecution.ActionAttach, Operation: workexecution.Attach,
		Owner: &directCompletion{inputs: inputs}}}, nil
}

func (owner *directCompletion) PrepareFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
	_ time.Time,
) (workqueue.Change, error) {
	if len(finished.Items) != 1 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach completion requires one connection")
	}
	guards, err := owner.inputs.DirectCompletionConditions(ctx, workqueue.View{
		Task: finished.Task, Item: finished.Items[0], ReadRevision: finished.ReadRevision,
	})
	return workqueue.Change{Conditions: guards}, err
}
