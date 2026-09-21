package hierarchydeletion

import (
	"context"
	"encoding/json"
	"errors"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"net/http"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	etcdinfra "github.com/AlanD20/groundplane/internal/infra/etcd"
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
		TargetKind: request.TargetKind, TargetID: request.TargetID, IdempotencyKey: request.IdempotencyKey,
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

func (repository *EtcdRepository) resolveReplay(
	ctx context.Context,
	request DeleteRequest,
	protected requestidempotency.ProtectedEvidence,
	expected *idempotencyrecord.IdempotencyLocator,
	mustFind bool,
) (BeginResult, bool, error) {
	intent := idempotencyIntent(request.TargetKind, request.TargetID)
	locator, revision, found, err := repository.lookupReplay(ctx, intent, request.TargetID, request.IdempotencyKey)
	if err != nil {
		return BeginResult{}, found, err
	}
	if !found {
		if mustFind {
			return BeginResult{}, true, errs.New(errs.KindInternal, "hierarchy deletion replay index is missing")
		}
		return BeginResult{}, false, nil
	}
	if expected != nil && *expected != locator {
		return BeginResult{}, true, errs.New(errs.KindInternal, "hierarchy deletion replay locator is inconsistent")
	}
	replayed, err := repository.replayAtRevision(ctx, request, protected, locator, revision)
	return replayed, true, err
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

func (repository *EtcdRepository) resolveReplayAfterMiss(
	ctx context.Context,
	request DeleteRequest,
	protected requestidempotency.ProtectedEvidence,
	expected idempotencyrecord.IdempotencyLocator,
) (BeginResult, bool, error) {
	evidence, revision, err := repository.idempotency.ReadWithRevision(ctx, expected)
	if err != nil {
		return BeginResult{}, false, err
	}
	if evidence == nil {
		return BeginResult{}, false, nil
	}
	marker, err := evidence.Marker()
	if err != nil {
		return BeginResult{}, true, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.Locator != expected || marker.TaskID == "" {
		return BeginResult{}, true, errs.New(errs.KindInternal, "hierarchy deletion replay marker is inconsistent")
	}
	replayTarget, err := replayTargetForIntent(
		idempotencyIntent(request.TargetKind, request.TargetID),
		request.TargetID,
	)
	if err != nil {
		return BeginResult{}, true, err
	}
	locator, found, err := repository.idempotency.ResolveReplayLocatorAtSnapshot(
		ctx,
		replayTarget,
		idempotencyIntent(request.TargetKind, request.TargetID).Method,
		idempotencyIntent(request.TargetKind, request.TargetID).RouteTemplate,
		request.IdempotencyKey,
		revision,
	)
	if err != nil {
		return BeginResult{}, true, err
	}
	if found {
		if locator != expected {
			return BeginResult{}, true, errs.New(
				errs.KindInternal,
				"hierarchy deletion replay locator is inconsistent",
			)
		}
		replayed, replayErr := repository.replayAtRevision(ctx, request, protected, locator, revision)
		return replayed, true, replayErr
	}
	resolution, err := repository.coordinator.ResolveMarker(ctx, protected, marker)
	if err != nil {
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindIdempotencyInProgress {
			return BeginResult{}, true, errs.New(
				errs.KindInternal,
				"hierarchy deletion idempotency marker has no replay index",
			)
		}
		return BeginResult{}, true, err
	}
	if resolution.Kind != requestidempotency.ResolutionReplay {
		return BeginResult{}, true, errs.New(errs.KindInternal, "hierarchy deletion replay resolution is invalid")
	}
	return BeginResult{}, true, errs.New(
		errs.KindInternal,
		"hierarchy deletion idempotency marker has no replay index",
	)
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

func isUnknownBeginError(err error) bool {
	kind, isKind := errs.KindOf(err)
	return errors.Is(err, context.DeadlineExceeded) || (isKind && kind == errs.KindStorageUnavailable)
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
		Query: requestidempotency.Object(), Body: requestidempotency.NoBody(),
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
		return requestidempotency.ProtectedEvidence{}, idempotencyrecord.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: idempotencyrecord.IdempotencyLocator{ScopeKind: idempotencyrecord.IdempotencyScopeKind(scopeKind), ScopeID: scopeID,
			Method: begin.IdempotencyIntent.Method, Route: begin.IdempotencyIntent.RouteTemplate, Key: begin.IdempotencyKey},
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

func replayRequestFromBegin(begin BeginDeletion) (DeleteRequest, error) {
	var target TargetKind
	switch begin.IdempotencyIntent.RouteTemplate {
	case TenantDeleteRoute:
		target = TargetTenant
	case ProjectDeleteRoute:
		target = TargetProject
	case EnvironmentDeleteRoute:
		target = TargetEnvironment
	default:
		return DeleteRequest{}, errs.New(errs.KindValidationFailed, "hierarchy deletion replay route is invalid")
	}
	return DeleteRequest{TargetKind: target, TargetID: begin.TargetID, IdempotencyKey: begin.IdempotencyKey}, nil
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
	operation, err := repository.journal.OperationByTaskAtRevision(ctx, response.TaskID, revision)
	if err != nil {
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindTaskNotFound {
			return BeginResult{}, errs.New(errs.KindInternal, "hierarchy deletion replay Task is missing")
		}
		return BeginResult{}, err
	}
	if locator.Method != http.MethodDelete ||
		locator.Route != idempotencyIntent(request.TargetKind, request.TargetID).RouteTemplate ||
		operation.MarkerLocator != locator ||
		operation.Tombstone.TargetID != request.TargetID ||
		operation.RootTaskID != markerTaskID ||
		!replayScopeMatches(locator, operation) ||
		(request.TargetKind == TargetTenant && operation.Tombstone.TargetKind != hierarchydeletion.HierarchyDeletionTargetTenant) ||
		(request.TargetKind == TargetEnvironment && operation.Tombstone.TargetKind != hierarchydeletion.HierarchyDeletionTargetEnvironment) ||
		(request.TargetKind == TargetBackingService && operation.Tombstone.TargetKind != hierarchydeletion.HierarchyDeletionTargetBacking) ||
		(request.TargetKind == TargetProject && operation.Tombstone.TargetKind != hierarchydeletion.HierarchyDeletionTargetProject) {
		return BeginResult{}, errs.New(errs.KindInternal, "hierarchy deletion replay proof is inconsistent")
	}
	converted := operationFromEtcd(operation)
	converted.TaskID = response.TaskID
	return BeginResult{Operation: converted, Existing: true}, nil
}

func replayScopeMatches(locator idempotencyrecord.IdempotencyLocator, operation etcdinfra.HierarchyDeletionOperation) bool {
	owner := operation.Owner
	if owner.WorkspaceType == etcdinfra.TaskWorkspacePlatform {
		return locator.ScopeKind == idempotencyrecord.IdempotencyScopePlatform && locator.ScopeID == "-"
	}
	if owner.WorkspaceType != etcdinfra.TaskWorkspaceTenant || owner.TenantID == "" {
		return false
	}
	if owner.EnvironmentID != "" {
		return locator.ScopeKind == idempotencyrecord.IdempotencyScopeProject && locator.ScopeID == owner.ProjectID
	}
	return locator.ScopeKind == idempotencyrecord.IdempotencyScopeTenant && locator.ScopeID == owner.TenantID
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

func replayTargetForIntent(intent IdempotencyIntent, targetID string) (idempotencyrecord.IdempotencyReplayTarget, error) {
	var kind idempotencyrecord.IdempotencyReplayTargetKind
	switch intent.RouteTemplate {
	case TenantDeleteRoute:
		kind = idempotencyrecord.IdempotencyReplayTargetTenant
	case ProjectDeleteRoute:
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
