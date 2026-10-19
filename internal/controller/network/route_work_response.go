package network

import (
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// routeWorkResponse binds replay to the accepted Task, not to a completed
// metadata write. Queue settlement owns completion of this marker.
func routeWorkResponse(
	claim blueprints.EnvironmentBlueprintStageClaim,
	record routes.Record,
) (idempotency.IdempotencyMarker, error) {
	if blueprints.ValidateEnvironmentBlueprintStageClaim(claim) != nil || routes.ValidateRecord(record) != nil ||
		claim.EnvironmentID != record.EnvironmentID || claim.TaskID != claim.RevisionID ||
		claim.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment || claim.Locator.ScopeID != record.EnvironmentID {
		return idempotency.IdempotencyMarker{}, errs.New(
			errs.KindValidationFailed,
			"Route acceptance does not match its staged action",
		)
	}
	body, err := json.Marshal(api.RouteTaskAccepted{Route: routeAPIResponse(record), TaskID: claim.TaskID})
	if claim.Locator.Method == http.MethodDelete {
		body, err = json.Marshal(api.TaskAccepted{TaskID: claim.TaskID})
	}
	if err != nil {
		return idempotency.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	intent := claim.Intent
	intent.Ciphertext = append([]byte(nil), intent.Ciphertext...)
	return idempotency.IdempotencyMarker{
		Kind: idempotency.IdempotencyMarkerTask, State: idempotency.IdempotencyMarkerPending,
		Locator: claim.Locator, Intent: intent, TaskID: claim.TaskID,
		ReplayTarget: &idempotency.IdempotencyReplayTarget{
			Kind: idempotency.IdempotencyReplayTargetRoute,
			ID:   record.Desired.ID,
		},
		Response: idempotency.IdempotencyResponse{
			Status:      http.StatusAccepted,
			ContentKind: "application/json",
			Body:        body,
		},
		CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}, nil
}
