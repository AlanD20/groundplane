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

// FailScriptWork accepts failure only from the active authenticated executor.
// The same captured-evidence settlement serves process interruption: neither
// a worker error nor process replacement is evidence of external absence.
func (repository *ScriptRepository) FailScriptWork(ctx context.Context, view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization, at time.Time,
) error {
	if !view.CurrentExecutor || !view.ActiveAttempt || view.ExecutorRevision <= 0 || view.Attempt.Attempt.Executor != authorization.Identity() {
		return errs.New(errs.KindStateConflict, "Script failure no longer owns its executor")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	guards = append(guards, keyvalue.Condition{Key: workqueue.ExecutorKey(authorization.Identity()), ModRevision: view.ExecutorRevision})
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		return repository.settleInterruptedScriptRecovery(ctx, view, guards, at)
	}
	return repository.settleInterruptedScript(ctx, view, guards, at)
}
