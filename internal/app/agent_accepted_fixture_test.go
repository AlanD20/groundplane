package app

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The lifecycle and image-fence journeys start with a real encrypted pure
// acceptance, then promote that same Task through the native authority factory.
// They exercise the native execution/recovery layer rather than fake readiness.
func acceptAgentFixture(t *testing.T, ctx context.Context, store keyvalue.Store,
	repository *etcd.TaskRepository, capture agentoperations.Capture, marker idempotency.IdempotencyMarker,
) error {
	t.Helper()
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		return err
	}
	replay, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		return err
	}
	protector, encrypted := manualJourneyEncryptedValue(t, "agent-request")
	clear(encrypted)
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		return err
	}
	submitter, err := newAcceptedActions(store, protector, queue, replay, coordinator)
	if err != nil {
		return err
	}
	intent, err := submitter.Begin(ctx, requestidempotency.CanonicalIntentV1{
		Method: marker.Locator.Method, Route: marker.Locator.Route,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: capture.AgentID}},
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "image", Value: requestidempotency.String(capture.Image)},
		)),
	}, marker.Locator)
	if err != nil {
		return err
	}
	defer intent.Destroy()
	task := workexecution.Task{ID: capture.TaskID, Actor: workexecution.OperatorActor, Action: capture.Action,
		TargetID: capture.AgentID, TargetName: "Agent", CreatedAt: capture.CreatedAt, UpdatedAt: capture.CreatedAt,
		Deadline: capture.CreatedAt.Add(acceptedaction.QueueTimeout)}
	request := struct {
		Image string `json:"image"`
	}{Image: capture.Image}
	if _, err := acceptedaction.Submit(ctx, submitter, intent.Submission(task, "agent-update"), request); err != nil {
		return err
	}
	accepted, _, err := queue.ReadTask(ctx, task.ID, 0)
	if err != nil {
		return err
	}
	_, item, err := capture.Work()
	if err != nil {
		return err
	}
	authority, err := repository.PrepareAgentWorkAuthority(ctx, capture)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(authority.Mutations)
	prepared := accepted.Task
	prepared.Intent, prepared.ExpectedItems = *item.Input, 1
	prepared.UpdatedAt = capture.CreatedAt
	promotion, err := queue.PrepareAcceptedTaskPromotion(ctx, accepted, prepared,
		[]workexecution.Item{item}, nil, false, capture.CreatedAt.Add(time.Nanosecond))
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(promotion.Mutations)
	result, err := queue.Commit(ctx, promotion, authority)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Agent lifecycle authority changed before promotion")
	}
	return nil
}
