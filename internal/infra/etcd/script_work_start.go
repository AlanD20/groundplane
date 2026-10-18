package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareScriptWorkStart joins the immutable Script/source authority to the
// dispatcher's atomic Start. It neither runs the command nor grants a Script
// effect: that still requires the durable start-authorized checkpoint.
func (repository *ScriptRepository) PrepareScriptWorkStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	attempt := view.Attempt.Attempt
	item := view.Work.Item.Item
	if !view.CurrentExecutor || !view.ActiveAttempt ||
		workexecution.ValidateDelivery(item, attempt) != nil ||
		attempt.Direction != workexecution.Forward || attempt.StartedAt != nil || attempt.FinishedAt != nil ||
		view.Work.Task.Task.Stop != nil || item.ForwardRevoked {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script no longer owns an unstarted forward attempt")
	}
	input, err := repository.LoadRetainedScriptWork(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer input.Execution.Clear()
	execution := input.Execution
	// Reconnecting an attempt goes through Start replay. A new attempt must
	// never adopt an execution whose command may already have been authorized.
	if execution.State != scriptexecutions.ScriptExecutionNotStarted || execution.StartAuthorized ||
		execution.AttemptID != "" || execution.Executor != nil || execution.ReconciliationRequired {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script has prior execution evidence and cannot start again")
	}
	return workqueue.Change{Conditions: input.Conditions}, nil
}
