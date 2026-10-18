package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SettleInterruptedScriptCompletion records only an already-proven outcome.
// The replacement process cannot adopt the old attempt or invoke its command.
func (repository *ScriptRepository) SettleInterruptedScriptCompletion(ctx context.Context, view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization, at time.Time,
) error {
	guards, err := repository.scriptReplacementConditions(ctx, view, authorization)
	if err != nil {
		return err
	}
	return repository.settleProvenScriptWork(ctx, view, guards, at)
}

func (repository *ScriptRepository) scriptReplacementConditions(ctx context.Context, view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization,
) ([]keyvalue.Condition, error) {
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	replacement, prior := authorization.Identity(), view.Attempt.Attempt.Executor
	if view.CurrentExecutor || replacement.Executor != workexecution.Agent || prior.Executor != workexecution.Agent ||
		replacement.HostID != prior.HostID || replacement.Generation <= prior.Generation {
		return nil, errs.New(errs.KindStateConflict, "Script reconciliation requires a replacement Agent on the same host")
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return nil, err
	}
	current, err := queue.ReadExecutor(ctx, view.Work.Item.Item.Placement)
	if err != nil {
		return nil, err
	}
	if current.Revision != view.ExecutorRevision || current.Record.Identity != replacement {
		return nil, errs.New(errs.KindStateConflict, "replacement Agent changed before Script completion")
	}
	guards = append(guards, keyvalue.Condition{Key: workqueue.ExecutorKey(replacement), ModRevision: current.Revision})
	return guards, nil
}
