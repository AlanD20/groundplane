package gitdeliveryoperator

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/gitselector"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type mutation struct {
	scope, scopeID string
	method, route  string
	key            string
	path           []requestidempotency.PathBinding
	expected       int64
	body           canonicalMutationBody
	status         int
}

type canonicalMutationBody struct {
	json []byte
	err  error
}

type mutationBodyInput interface {
	api.GitHubConnectionCreateRequest | api.GitHubConnectionCredentialsWrite |
		api.GitDeploymentBindingWrite | api.GitDeploymentStartRequest | *gitselector.Selector
}

func encodeMutationBody[T mutationBodyInput](input T) canonicalMutationBody {
	encoded, err := json.Marshal(input)
	return canonicalMutationBody{json: encoded, err: err}
}

type publish func(response any) ([]keyvalue.Condition, []keyvalue.Mutation, error)

func (operator *Operator) mutate(ctx context.Context, request mutation,
	apply func(publish) (int64, error), finalize func(*idempotencyrecord.IdempotencyResponse, int64) error,
) (idempotencyrecord.IdempotencyResponse, error) {
	body := request.body.json
	defer clear(body)
	if request.body.err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "encode GitHub deployment request")
	}
	if body == nil {
		body = []byte("{}")
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: request.method, Route: request.route,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeKind(request.scope), ID: request.scopeID},
		Path:  request.path,
		Query: requestidempotency.Object(requestidempotency.Field{
			Name: "expected_revision", Value: requestidempotency.Integer(request.expected),
		}),
		Body: requestidempotency.JSONBody(requestidempotency.Object(requestidempotency.Field{
			Name: "request", Value: requestidempotency.String(string(body)),
		})),
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := operator.idempotency.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeKind(request.scope), ScopeID: request.scopeID,
		Method: request.method, Route: request.route, Key: request.key,
	}
	read := func(readCtx context.Context) (idempotencyrecord.IdempotencyResponse, bool, error) {
		record, err := operator.replays.Read(readCtx, locator)
		if err != nil || record == nil {
			return idempotencyrecord.IdempotencyResponse{}, false, err
		}
		marker, err := record.Marker()
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, true, err
		}
		result, err := operator.idempotency.ResolveMarker(readCtx, evidence, marker)
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, true, err
		}
		response := result.Response
		response.Status = request.status
		if finalize != nil {
			err = finalize(&response, record.Revision())
		}
		return response, true, err
	}
	if response, found, err := read(ctx); err != nil || found {
		return response, err
	}
	var accepted idempotencyrecord.IdempotencyResponse
	revision, err := apply(func(result any) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
		response := idempotencyrecord.IdempotencyResponse{Status: request.status, ContentKind: "application/json"}
		if request.status == http.StatusNoContent {
			response.ContentKind = "none"
		} else {
			body, marshalErr := json.Marshal(result)
			if marshalErr != nil {
				return nil, nil, errs.New(errs.KindInternal, "encode GitHub deployment response")
			}
			response.Body = body
		}
		// The direct marker owns the synchronous acceptance receipt. Translate
		// its 200 status back to 202 for the asynchronous operator endpoints.
		if response.Status == http.StatusAccepted {
			response.Status = http.StatusOK
		}
		durable, protectErr := evidence.DurableRecord()
		if protectErr != nil {
			return nil, nil, protectErr
		}
		defer clear(durable.Ciphertext)
		marker, markerErr := idempotencyrecord.NewCompletedDirectIdempotencyMarker(
			locator,
			durable,
			response,
			time.Now().UTC(),
		)
		if markerErr != nil {
			return nil, nil, markerErr
		}
		defer clear(marker.Intent.Ciphertext)
		conditions, mutations, err := idempotencyrecord.PreparePublication(marker)
		if err == nil {
			accepted = response
			accepted.Status = request.status
		}
		return conditions, mutations, err
	})
	if err == nil && accepted.Status != 0 {
		if finalize != nil {
			err = finalize(&accepted, revision)
		}
		return accepted, err
	}
	// A transaction racing an equal request can lose, and an unavailable
	// response can conceal commitment. Recover the retained exact receipt.
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	response, found, replayErr := read(readCtx)
	if found {
		return response, replayErr
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if replayErr != nil {
		return idempotencyrecord.IdempotencyResponse{}, replayErr
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"GitHub deployment acceptance has no durable receipt",
	)
}

func pathBinding(name, value string) requestidempotency.PathBinding {
	return requestidempotency.PathBinding{Name: name, Value: value}
}
