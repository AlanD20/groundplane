package network

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ExpireUnstarted records a deadline failure without granting or renewing Start.
// The failure and completion compare the attempt and checkpoint revisions, so a
// concurrent Start cannot be mistaken for proof that no host command ran.
func (owner *AgentWork) ExpireUnstarted(ctx context.Context, view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization, at time.Time,
) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	attempt := view.Attempt.Attempt
	if authorization.Identity() != attempt.Executor || !view.CurrentExecutor || !view.ActiveAttempt ||
		attempt.StartedAt != nil || view.Checkpoint.Record != nil || at.Before(attempt.Deadline) ||
		view.Work.Item.Item.Operation != workexecution.Network {
		return errs.New(errs.KindStateConflict, "Network deadline settlement requires an expired unstarted assignment")
	}
	proof, result, err := owner.inputs.PrepareFailure(ctx, view, networkexecution.DeadlineExpired, at, false)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(proof.Mutations)
	completion, err := owner.queue.ReadCompletionAt(ctx, view)
	if err != nil {
		return err
	}
	completion.Result, completion.At = result, at
	change, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	committed, err := owner.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "Network assignment changed before deadline settlement")
	}
	return nil
}
