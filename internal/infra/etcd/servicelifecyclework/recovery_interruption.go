package servicelifecyclework

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func RecoveryInterruptionKey(attemptID string) string {
	return "/v1/execution/service-lifecycle/recovery-interruptions/" + attemptID
}

// This is Controller evidence of executor loss, not a fabricated Agent
// observation. It never asserts that host effects stopped or cleanup succeeded.
func (repository *Repository) PrepareRecoveryInterruption(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.ServiceLifecycleWorkInput, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	attempt := view.Attempt.Attempt
	if view.CurrentExecutor || !view.ActiveAttempt || view.ExecutorRevision <= 0 ||
		attempt.Direction != workexecution.Recovery ||
		attempt.FinishedAt != nil ||
		at.Before(attempt.AssignedAt) ||
		!recordcodec.IsCanonicalUTC(at) ||
		view.Work.Task.Task.CollectionStartedAt != nil {
		return workqueue.Change{}, workexecution.Completion{}, conflict(
			"Service lifecycle recovery interruption lacks replaced execution ownership",
		)
	}
	captured, guard, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if !sameCapture(captured, captured, input) {
		return workqueue.Change{}, workexecution.Completion{}, conflict("Service lifecycle recovery input changed")
	}
	source, err := repository.LoadRecovery(ctx, view.Work, input)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	if err := source.Source.ValidateFor(attempt, input); err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	raw, err := recordcodec.Encode("service_lifecycle_recovery_interruption", struct {
		AttemptID      string              `json:"attempt_id"`
		Input          workexecution.Input `json:"input"`
		ForwardFailure workexecution.Input `json:"forward_failure"`
		At             time.Time           `json:"at"`
	}{attempt.ID, attempt.Input, source.Source.Evidence, at})
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	digest := sha256.Sum256(raw)
	evidence := workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, attempt.AssignedAt, attempt.ID, "lifecycle-recovery-interrupted"),
		SHA256: hex.EncodeToString(digest[:]),
	}
	key := RecoveryInterruptionKey(attempt.ID)
	conditions := []keyvalue.Condition{guard,
		{Key: key}, {Key: RecoveryKey(attempt.WorkItemID), ModRevision: source.Revision},
		{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision},
	}
	return workqueue.Change{
			Conditions: conditions,
			Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}},
		},
		workexecution.Completion{
			Outcome:        workexecution.Failed,
			Evidence:       evidence,
			EffectsSettled: false,
		}, nil
}
