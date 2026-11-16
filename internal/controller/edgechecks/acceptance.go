package edgechecks

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

func acceptCheck[T any](ctx context.Context, service *Service, environmentID, targetID, targetName, kind string,
	after []string, input T, locator idempotency.IdempotencyLocator, evidence requestidempotency.ProtectedEvidence,
) (idempotency.IdempotencyResponse, error) {
	scope, err := service.routes.ConnectionCheckScope(ctx, environmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	at := time.Now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionCheck,
		Scope:      scope,
		TargetID:   targetID,
		TargetName: targetName,
		After:      append([]string(nil), after...),
		CreatedAt:  at,
		UpdatedAt:  at,
		Deadline:   at.Add(acceptedaction.QueueTimeout),
	}
	durable, err := evidence.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(durable.Ciphertext)
	return acceptedaction.Submit(
		ctx,
		service.submitter,
		acceptedaction.Submission{Task: task, Kind: kind, Locator: locator, Intent: durable, Evidence: evidence},
		input,
	)
}
