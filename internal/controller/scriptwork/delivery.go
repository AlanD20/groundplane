package scriptwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// LoadWorkAssignment resolves captured generations, never current desired
// configuration. A reconnect receives the same attempt and its saved checkpoint.
// The caller authenticates the executor and clears plaintext after sending.
func (service *Service) LoadWorkAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.ScriptWorkAssignment, error) {
	attempt := view.Attempt.Attempt
	if !view.ActiveAttempt || !view.CurrentExecutor || attempt.Direction != workexecution.Forward ||
		attempt.FinishedAt != nil || workexecution.ValidateDelivery(view.Work.Item.Item, attempt) != nil {
		return nil, errs.New(errs.KindStateConflict, "Script delivery does not own an active forward assignment")
	}
	delivery, err := service.artifacts.ResolveScriptWorkArtifacts(ctx, view.Work)
	if delivery != nil {
		defer delivery.Clear()
	}
	if err != nil {
		return nil, err
	}
	if delivery == nil || delivery.Captured == nil || delivery.Checkpoint == nil {
		return nil, errs.New(errs.KindInternal, "Script captured delivery is incomplete")
	}
	execution := delivery.Captured.Input.Execution
	if execution.Input != attempt.Input || execution.AttemptID != "" &&
		(execution.AttemptID != attempt.ID || execution.Executor == nil || *execution.Executor != attempt.Executor) {
		return nil, errs.New(errs.KindStateConflict, "Script execution belongs to another attempt")
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	return proto.CloneOf(&agentpb.ScriptWorkAssignment{
		Attempt: identity, OperationId: execution.OperationID,
		Run: delivery.Captured.Run, Snapshot: delivery.Captured.Snapshot,
		Projection: delivery.Captured.Projection, Body: delivery.Captured.Body,
		Entries: delivery.Entries, Checkpoint: delivery.Checkpoint,
	}), nil
}
