package remotehost

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	record "github.com/AlanD20/groundplane/internal/common/remotehost"
	request "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type humanMutation struct {
	method, route, key, hostID string
	status                     int
	fields                     []request.Field
}

type hostChange struct {
	previous record.VersionedHost
	next     record.Host
	response []byte
}

func withHostResponse[T api.RemoteHost | api.RemoteHostEnrollment](change hostChange, value T) (hostChange, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return hostChange{}, errs.Wrap(errs.KindInternal, err)
	}
	change.response = encoded
	return change, nil
}

func (operator *Operator) mutate(ctx context.Context, input humanMutation, prepare func() (hostChange, error)) (
	idempotency.IdempotencyResponse, error,
) {
	locator := idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopePlatform, ScopeID: "-",
		Method: input.method, Route: input.route, Key: input.key,
	}
	if err := idempotency.ValidateIdempotencyLocator(locator); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	canonical := request.CanonicalIntentV1{
		Method: input.method, Route: input.route, Scope: request.Scope{Kind: request.ScopePlatform},
		Query: request.Object(), Body: request.JSONBody(request.Object(input.fields...)),
	}
	if input.hostID != "" {
		canonical.Path = []request.PathBinding{{Name: "id", Value: input.hostID}}
	}
	version, digest, err := request.Canonicalize(ctx, canonical)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := operator.intents.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	read := func(ctx context.Context) (idempotency.IdempotencyResponse, bool, error) {
		prior, found, err := operator.intents.ResolveExisting(ctx, operator.replays, locator, evidence)
		if err != nil || !found {
			return idempotency.IdempotencyResponse{}, found, err
		}
		return request.CloneResponse(prior.Response), true, nil
	}
	if response, found, err := read(ctx); found || err != nil {
		return response, err
	}
	change, err := prepare()
	if err == nil {
		response, applyErr := operator.publish(ctx, locator, evidence, input.status, change)
		if applyErr == nil {
			return response, nil
		}
		err = applyErr
	}
	// An equal concurrent submission or an interrupted response can conceal a
	// committed ceremony. Recover the original public certificate, not a new key.
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if response, found, replayErr := read(readCtx); found || replayErr != nil {
		return response, replayErr
	}
	return idempotency.IdempotencyResponse{}, err
}

func (operator *Operator) publish(ctx context.Context, locator idempotency.IdempotencyLocator,
	evidence request.ProtectedEvidence, status int, change hostChange,
) (idempotency.IdempotencyResponse, error) {
	response := idempotency.IdempotencyResponse{Status: status, ContentKind: "none"}
	if status != http.StatusNoContent {
		response.ContentKind, response.Body = "application/json", change.response
	}
	protected, err := evidence.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(protected.Ciphertext)
	marker, err := idempotency.NewCompletedDirectIdempotencyMarker(
		locator,
		protected,
		response,
		operator.service.now().UTC(),
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	conditions, mutations, err := idempotency.PreparePublication(marker)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer keyvalue.ClearMutationValues(mutations)
	applied, err := operator.repository.PublishOperatorChange(ctx, change.previous, change.next,
		workqueue.Change{Conditions: conditions, Mutations: mutations})
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if !applied {
		return idempotency.IdempotencyResponse{}, staleHost()
	}
	return response, nil
}
