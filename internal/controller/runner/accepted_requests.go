package runner

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
)

func newRunnerAcceptedTask(desired runners.RunnerDesiredRecord, actor workexecution.Actor,
	action workexecution.Action, after []string, at time.Time,
) workexecution.Task {
	return workexecution.Task{ID: ids.New(ids.KindTask), Actor: actor, Action: action,
		TargetID: desired.ID, TargetName: desired.Slug,
		Scope: workexecution.Scope{
			TenantID:      desired.TenantID,
			ProjectID:     desired.ProjectID(),
			EnvironmentID: desired.EnvironmentID(),
		},
		After: append(
			[]string(nil),
			after...), CreatedAt: at, UpdatedAt: at, Deadline: at.Add(acceptedaction.QueueTimeout)}
}

func submitRunnerAction[T any](ctx context.Context, submitter *acceptedaction.Submitter, task workexecution.Task,
	kind string, locator idempotency.IdempotencyLocator, evidence requestidempotency.ProtectedEvidence, input T,
) (idempotency.IdempotencyResponse, error) {
	durable, err := evidence.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	return acceptedaction.Submit(ctx, submitter, acceptedaction.Submission{
		Task: task, Kind: kind, Locator: locator, Intent: durable, Evidence: evidence,
		ReplayTarget: &idempotency.IdempotencyReplayTarget{
			Kind: idempotency.IdempotencyReplayTargetRunner,
			ID:   task.TargetID,
		},
	}, input)
}

func runnerNativeItem(task workexecution.Task, workID string, operation workexecution.Operation,
	at time.Time, timeout time.Duration,
) workexecution.Item {
	return workexecution.Item{ID: workID, TaskID: task.ID, Operation: operation,
		TargetID: task.TargetID, TargetName: task.TargetName, Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: task.TargetID}}, Input: &task.Intent,
		Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: at, UpdatedAt: at, Deadline: acceptedaction.ExecutionDeadline(task, at, timeout)}
}
