package agentmanagement

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func beginAgentAction(
	ctx context.Context,
	submitter *acceptedaction.Submitter,
	method, route, agentID string,
	body requestidempotency.Value,
	key string,
) (acceptedaction.Intent, error) {
	if ctx == nil {
		return acceptedaction.Intent{}, errs.New(errs.KindInternal, "Agent action context is required")
	}
	var path []requestidempotency.PathBinding
	if agentID != "" {
		path = []requestidempotency.PathBinding{{Name: "id", Value: agentID}}
	}
	return submitter.Begin(ctx, requestidempotency.CanonicalIntentV1{Method: method, Route: route,
		Scope: requestidempotency.Scope{
			Kind: requestidempotency.ScopePlatform,
		}, Path: path, Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(
			body,
		)}, idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopePlatform, ScopeID: "-", Method: method, Route: route, Key: key})
}

func newAcceptedAgentTask(
	at time.Time,
	agentID string,
	action workexecution.Action,
	after []string,
) workexecution.Task {
	return workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor, Action: action,
		TargetID: agentID, TargetName: "Agent", After: append([]string(nil), after...), CreatedAt: at, UpdatedAt: at, Deadline: at.Add(acceptedaction.QueueTimeout)}
}

func prepareAgentCapture(
	ctx context.Context,
	repository agentCaptureRepository,
	accepted workqueue.TaskSnapshot,
	capture agentoperations.Capture,
	at time.Time,
) (taskdependencies.Prepared, error) {
	_, item, err := capture.Work()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task := accepted.Task
	task.Intent = *item.Input
	task.ExpectedItems = 1
	task.UpdatedAt = at
	timeout := 120 * time.Second
	if capture.Action == workexecution.ActionUpdate {
		timeout = 300 * time.Second
	}
	item.Deadline = acceptedaction.ExecutionDeadline(task, at, timeout)
	change, err := repository.PrepareAgentWorkAuthority(ctx, capture)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: change}, nil
}
