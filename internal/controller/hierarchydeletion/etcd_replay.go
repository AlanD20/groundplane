package hierarchydeletion

import (
	"context"
	"encoding/json"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"net/http"

	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EtcdRepository) ResolveDeletionReplay(
	ctx context.Context,
	request DeleteRequest,
) (BeginResult, bool, error) {
	intent := idempotencyIntent(request.TargetKind, request.TargetID)
	locator, revision, found, err := repository.lookupReplay(ctx, intent, request.TargetID, request.IdempotencyKey)
	if err != nil || !found {
		return BeginResult{}, found, err
	}
	scopeKind, scopeID, err := domainScopeFromMarker(locator)
	if err != nil {
		return BeginResult{}, true, err
	}
	protected, candidate, err := repository.protectBegin(ctx, BeginDeletion{
		TargetKind: request.TargetKind, TargetID: request.TargetID, IdempotencyKey: request.IdempotencyKey, After: request.After,
		IdempotencyIntent: idempotencyIntentWithScope(request.TargetKind, request.TargetID, scopeKind, scopeID),
	})
	if err != nil {
		return BeginResult{}, true, err
	}
	defer protected.Destroy()
	defer clear(candidate.Intent.Ciphertext)
	defer clear(candidate.Response.Body)
	replayed, err := repository.replayAtRevision(ctx, request, protected, locator, revision)
	return replayed, true, err
}

func (repository *EtcdRepository) lookupReplay(
	ctx context.Context,
	intent IdempotencyIntent,
	targetID string,
	key string,
) (idempotencyrecord.IdempotencyLocator, int64, bool, error) {
	replayTarget, err := replayTargetForIntent(intent, targetID)
	if err != nil {
		return idempotencyrecord.IdempotencyLocator{}, 0, false, err
	}
	return repository.idempotency.ResolveReplayLocatorAtRevision(
		ctx, replayTarget, intent.Method, intent.RouteTemplate, key,
	)
}

func (repository *EtcdRepository) protectBegin(
	ctx context.Context,
	begin BeginDeletion,
) (requestidempotency.ProtectedEvidence, idempotencyrecord.IdempotencyMarker, error) {
	replayTarget, err := replayTargetForIntent(begin.IdempotencyIntent, begin.TargetID)
	if err != nil {
		return requestidempotency.ProtectedEvidence{}, idempotencyrecord.IdempotencyMarker{}, err
	}
	scopeKind := idempotencyScopeKind(begin.IdempotencyIntent.ScopeKind)
	scopeID := begin.IdempotencyIntent.ScopeID
	canonicalScopeID := scopeID
	if scopeKind == requestidempotency.ScopePlatform {
		canonicalScopeID = ""
		scopeID = "-"
	}
	intent := requestidempotency.CanonicalIntentV1{
		Method: begin.IdempotencyIntent.Method, Route: begin.IdempotencyIntent.RouteTemplate,
		Scope: requestidempotency.Scope{Kind: scopeKind, ID: canonicalScopeID},
		Path:  make([]requestidempotency.PathBinding, len(begin.IdempotencyIntent.PathBindings)),
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(begin.After)})),
	}
	for index, binding := range begin.IdempotencyIntent.PathBindings {
		intent.Path[index] = requestidempotency.PathBinding{Name: binding.Name, Value: binding.Value}
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, intent)
	if err != nil {
		return requestidempotency.ProtectedEvidence{}, idempotencyrecord.IdempotencyMarker{}, err
	}
	protected, err := repository.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return requestidempotency.ProtectedEvidence{}, idempotencyrecord.IdempotencyMarker{}, err
	}
	durable, err := protected.DurableRecord()
	if err != nil {
		protected.Destroy()
		return requestidempotency.ProtectedEvidence{}, idempotencyrecord.IdempotencyMarker{}, err
	}
	body, err := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: begin.TaskIDCandidate})
	if err != nil {
		protected.Destroy()
		clear(durable.Ciphertext)
		return requestidempotency.ProtectedEvidence{}, idempotencyrecord.IdempotencyMarker{}, errs.Wrap(
			errs.KindInternal,
			err,
		)
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: idempotencyrecord.IdempotencyLocator{
			ScopeKind: idempotencyrecord.IdempotencyScopeKind(scopeKind),
			ScopeID:   scopeID,
			Method:    begin.IdempotencyIntent.Method,
			Route:     begin.IdempotencyIntent.RouteTemplate,
			Key:       begin.IdempotencyKey,
		},
		ReplayTarget: &replayTarget, Intent: durable,
		Response: idempotencyrecord.IdempotencyResponse{
			Status:      http.StatusAccepted,
			ContentKind: "application/json",
			Body:        body,
		},
		TaskID: begin.TaskIDCandidate, CreatedAt: begin.CreatedAt, UpdatedAt: begin.CreatedAt,
	}
	return protected, marker, nil
}

func (repository *EtcdRepository) replayedBeginAtRevision(
	ctx context.Context,
	resolution requestidempotency.Resolution,
	revision int64,
	markerTaskID string,
	request DeleteRequest,
	locator idempotencyrecord.IdempotencyLocator,
) (BeginResult, error) {
	if resolution.Kind != requestidempotency.ResolutionReplay || resolution.Response.Status != http.StatusAccepted ||
		revision <= 0 ||
		markerTaskID == "" {
		return BeginResult{}, errs.New(errs.KindInternal, "hierarchy deletion replay response is invalid")
	}
	var response struct {
		TaskID string `json:"task_id"`
	}
	if json.Unmarshal(resolution.Response.Body, &response) != nil || response.TaskID == "" ||
		response.TaskID != markerTaskID {
		return BeginResult{}, errs.New(errs.KindInternal, "hierarchy deletion replay Task is inconsistent")
	}
	if locator.Method != "DELETE" ||
		locator.Route != idempotencyIntent(request.TargetKind, request.TargetID).RouteTemplate {
		return BeginResult{}, errs.New(errs.KindInternal, "hierarchy deletion replay locator is inconsistent")
	}
	return BeginResult{
		Operation: Operation{
			ID:     stableOperationID(operationForTarget(request.TargetKind), request.TargetID, request.IdempotencyKey),
			TaskID: response.TaskID,
		},
		Existing: true,
	}, nil
}

func idempotencyScopeKind(target TargetKind) requestidempotency.ScopeKind {
	switch target {
	case TargetTenant:
		return requestidempotency.ScopeTenant
	case TargetProject:
		return requestidempotency.ScopeProject
	case TargetEnvironment:
		return requestidempotency.ScopeEnvironment
	case TargetBackingService:
		return requestidempotency.ScopePlatform
	default:
		return ""
	}
}

func domainScopeKind(scope idempotencyrecord.IdempotencyScopeKind) (TargetKind, error) {
	switch scope {
	case idempotencyrecord.IdempotencyScopePlatform:
		return TargetBackingService, nil
	case idempotencyrecord.IdempotencyScopeTenant:
		return TargetTenant, nil
	case idempotencyrecord.IdempotencyScopeProject:
		return TargetProject, nil
	case idempotencyrecord.IdempotencyScopeEnvironment:
		return TargetEnvironment, nil
	default:
		return "", errs.New(errs.KindInternal, "hierarchy deletion replay scope is invalid")
	}
}

func domainScopeFromMarker(locator idempotencyrecord.IdempotencyLocator) (TargetKind, string, error) {
	scope, err := domainScopeKind(locator.ScopeKind)
	if err != nil {
		return "", "", err
	}
	if scope == TargetBackingService {
		if locator.ScopeID != "-" {
			return "", "", errs.New(errs.KindInternal, "hierarchy deletion platform replay scope is invalid")
		}
		return scope, "", nil
	}
	return scope, locator.ScopeID, nil
}

func replayTargetForIntent(
	intent IdempotencyIntent,
	targetID string,
) (idempotencyrecord.IdempotencyReplayTarget, error) {
	var kind idempotencyrecord.IdempotencyReplayTargetKind
	switch intent.RouteTemplate {
	case TenantDeleteRoute:
		kind = idempotencyrecord.IdempotencyReplayTargetTenant
	case ProjectDeleteRoute:
		kind = idempotencyrecord.IdempotencyReplayTargetProject
	case BackingDeleteRoute:
		kind = idempotencyrecord.IdempotencyReplayTargetProject
	case EnvironmentDeleteRoute:
		kind = idempotencyrecord.IdempotencyReplayTargetEnvironment
	default:
		return idempotencyrecord.IdempotencyReplayTarget{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion replay route is invalid",
		)
	}
	return idempotencyrecord.IdempotencyReplayTarget{Kind: kind, ID: targetID}, nil
}

func (repository *EtcdRepository) replayAtRevision(
	ctx context.Context,
	request DeleteRequest,
	protected requestidempotency.ProtectedEvidence,
	locator idempotencyrecord.IdempotencyLocator,
	revision int64,
) (BeginResult, error) {
	evidence, err := repository.idempotency.ReadAtRevision(ctx, locator, revision)
	if err != nil {
		return BeginResult{}, err
	}
	if evidence == nil {
		return BeginResult{}, errs.New(
			errs.KindInternal,
			"hierarchy deletion replay marker disappeared from its fixed snapshot",
		)
	}
	marker, err := evidence.Marker()
	if err != nil {
		return BeginResult{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.Locator != locator || marker.TaskID == "" {
		return BeginResult{}, errs.New(errs.KindInternal, "hierarchy deletion replay marker is inconsistent")
	}
	resolution, err := repository.coordinator.ResolveMarker(ctx, protected, marker)
	if err != nil {
		return BeginResult{}, err
	}
	return repository.replayedBeginAtRevision(ctx, resolution, revision, marker.TaskID, request, locator)
}
