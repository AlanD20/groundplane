package runneridentity

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	replayintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core/runneraccess"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnergrants"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const CLIGrantRoute = "/runners/{id}/cli-grant"

type GrantOperator struct {
	manager *GrantManager
	intents *replayintent.Coordinator
	replays *etcd.IdempotencyRepository
}

func NewGrantOperator(
	manager *GrantManager,
	intents *replayintent.Coordinator,
	replays *etcd.IdempotencyRepository,
) *GrantOperator {
	return &GrantOperator{manager: manager, intents: intents, replays: replays}
}

func (operator *GrantOperator) ReadCLIGrant(ctx context.Context, runnerID string) (api.RunnerCLIGrantStatus, error) {
	manager := operator.manager
	runner, err := manager.admission.runners.GetRunner(ctx, runnerID)
	if err != nil {
		return api.RunnerCLIGrantStatus{}, err
	}
	stored, err := manager.grants.Get(ctx, runnerID)
	if err != nil {
		return api.RunnerCLIGrantStatus{}, err
	}
	if runner.Record.RuntimeEpoch == 0 {
		return grantView(runnerID, stored, false, false), nil
	}
	digest, binding, _, err := manager.credentials.ReadActiveBaseCredential(ctx, runnerID, runner.Record.RuntimeEpoch)
	if err != nil {
		return api.RunnerCLIGrantStatus{}, err
	}
	available := false
	if binding.Revision > 0 {
		_, err = manager.migrations.Authority(ctx, runnerID)
		if err == nil {
			available = true
		} else if kind, ok := errs.KindOf(err); !ok ||
			kind != errs.KindStateConflict && kind != errs.KindResourceInUse && !errs.IsNotFound(err) {
			return api.RunnerCLIGrantStatus{}, err
		}
	}
	active := binding.Revision > 0 && stored.Record.RunnerEpoch == binding.RunnerEpoch &&
		stored.Record.TokenDigest == runnergrants.TokenDigest(digest) && stored.Record.ExpiresAt.After(time.Now())
	return grantView(runnerID, stored, active, available), nil
}

func (operator *GrantOperator) WriteCLIGrant(
	ctx context.Context, runnerID string, expected int64, body api.RunnerCLIGrantWrite, key string,
) (api.RunnerCLIGrantStatus, error) {
	if body.LifetimeSeconds < 1 || body.LifetimeSeconds > 3600 || expected < 0 {
		return api.RunnerCLIGrantStatus{}, errs.New(
			errs.KindValidationFailed,
			"Runner CLI grant lifetime or revision is invalid",
		)
	}
	return operator.mutate(ctx, runnerID, expected, key, http.MethodPut, &body)
}

func (operator *GrantOperator) RevokeCLIGrant(ctx context.Context, runnerID string, expected int64, key string) error {
	if expected <= 0 {
		return errs.New(errs.KindValidationFailed, "Runner CLI grant revocation requires the checked revision")
	}
	_, err := operator.mutate(ctx, runnerID, expected, key, http.MethodDelete, nil)
	return err
}

func grantView(
	runnerID string, stored keyvalue.Versioned[runnergrants.Record], active, available bool,
) api.RunnerCLIGrantStatus {
	view := api.RunnerCLIGrantStatus{
		RunnerID: runnerID, Revision: stored.Revision, Active: active, Available: available,
		Actions: []api.RunnerCLIGrantAction{},
	}
	if stored.Record.Revoked || stored.Record.ExpiresAt.IsZero() {
		return view
	}
	expiry := stored.Record.ExpiresAt
	view.ExpiresAt = &expiry
	view.Scope = &api.RunnerCLIGrantScope{
		TenantID: stored.Record.Scope.TenantID, ProjectID: stored.Record.Scope.ProjectID,
		EnvironmentID: stored.Record.Scope.EnvironmentID,
	}
	for _, action := range stored.Record.Actions {
		view.Actions = append(view.Actions, api.RunnerCLIGrantAction{
			Operation: action.Operation, TargetID: action.TargetID, Image: action.Image, TaskIDs: action.TaskIDs,
		})
	}
	return view
}

func (operator *GrantOperator) mutate(
	ctx context.Context, runnerID string, expected int64, key, method string, body *api.RunnerCLIGrantWrite,
) (api.RunnerCLIGrantStatus, error) {
	if err := ids.Validate(ids.KindRunner, runnerID); err != nil {
		return api.RunnerCLIGrantStatus{}, err
	}
	scope := replayintent.Scope{Kind: replayintent.ScopePlatform}
	locator := idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopePlatform, ScopeID: "-", Method: method, Route: CLIGrantRoute, Key: key,
	}
	if err := idempotency.ValidateIdempotencyLocator(locator); err != nil {
		return api.RunnerCLIGrantStatus{}, err
	}
	request, err := json.Marshal(body)
	if err != nil {
		return api.RunnerCLIGrantStatus{}, errs.Wrap(errs.KindInternal, err)
	}
	version, digest, err := replayintent.Canonicalize(ctx, replayintent.CanonicalIntentV1{
		Method: method, Route: CLIGrantRoute, Scope: scope, Path: []replayintent.PathBinding{{Name: "id", Value: runnerID}},
		Query: replayintent.Object(replayintent.Field{Name: "revision", Value: replayintent.Integer(expected)}),
		Body: replayintent.JSONBody(replayintent.Object(
			replayintent.Field{Name: "grant", Value: replayintent.String(string(request))},
		)),
	})
	if err != nil {
		return api.RunnerCLIGrantStatus{}, err
	}
	defer digest.Destroy()
	evidence, err := operator.intents.ProtectIntent(ctx, version, digest)
	if err != nil {
		return api.RunnerCLIGrantStatus{}, err
	}
	defer evidence.Destroy()
	if view, found, err := operator.receipt(ctx, locator, evidence); err != nil || found {
		return view, err
	}
	view, err := operator.publish(ctx, runnerID, expected, body, locator, evidence)
	if err == nil {
		return view, nil
	}
	recovery, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if replay, found, replayErr := operator.receipt(recovery, locator, evidence); found || replayErr != nil {
		return replay, replayErr
	}
	return api.RunnerCLIGrantStatus{}, err
}

func (operator *GrantOperator) publish(
	ctx context.Context, runnerID string, expected int64, body *api.RunnerCLIGrantWrite,
	locator idempotency.IdempotencyLocator, evidence replayintent.ProtectedEvidence,
) (api.RunnerCLIGrantStatus, error) {
	manager := operator.manager
	previous, err := manager.grants.Get(ctx, runnerID)
	if err != nil {
		return api.RunnerCLIGrantStatus{}, err
	}
	if previous.Revision != expected || body == nil && previous.Record.Revoked {
		return api.RunnerCLIGrantStatus{}, errs.New(
			errs.KindStateConflict,
			"Runner CLI grant changed after it was read",
		)
	}
	now := time.Now().UTC()
	record := runnergrants.Record{RunnerID: runnerID, Revoked: body == nil}
	var guards []keyvalue.Condition
	if body != nil {
		issue := GrantIssue{
			RunnerID: runnerID, Lifetime: time.Duration(body.LifetimeSeconds) * time.Second,
			Scope: workexecution.Scope{
				TenantID:      body.Scope.TenantID,
				ProjectID:     body.Scope.ProjectID,
				EnvironmentID: body.Scope.EnvironmentID,
			},
			Actions: make([]GrantAction, 0, len(body.Actions)),
		}
		for _, action := range body.Actions {
			issue.Actions = append(issue.Actions, GrantAction{
				Operation: runneraccess.Operation(
					action.Operation,
				), TargetID: action.TargetID, Image: action.Image, TaskIDs: action.TaskIDs,
			})
		}
		record, guards, err = manager.prepare(ctx, issue, now)
		if err != nil {
			return api.RunnerCLIGrantStatus{}, err
		}
	}
	view := grantView(
		runnerID,
		keyvalue.Versioned[runnergrants.Record]{Record: record},
		!record.Revoked,
		!record.Revoked,
	)
	response := idempotency.IdempotencyResponse{Status: http.StatusNoContent, ContentKind: "none"}
	if body != nil {
		content, err := json.Marshal(view)
		if err != nil {
			return api.RunnerCLIGrantStatus{}, errs.Wrap(errs.KindInternal, err)
		}
		response = idempotency.IdempotencyResponse{
			Status:      http.StatusOK,
			ContentKind: "application/json",
			Body:        content,
		}
	}
	protected, err := evidence.DurableRecord()
	if err != nil {
		return api.RunnerCLIGrantStatus{}, err
	}
	defer clear(protected.Ciphertext)
	marker, err := idempotency.NewCompletedDirectIdempotencyMarker(locator, protected, response, now)
	if err != nil {
		return api.RunnerCLIGrantStatus{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	conditions, mutations, err := idempotency.PreparePublication(marker)
	if err != nil {
		return api.RunnerCLIGrantStatus{}, err
	}
	defer keyvalue.ClearMutationValues(mutations)
	revision, err := manager.grants.Publish(ctx, expected, record, time.Now(), guards,
		workqueue.Change{Conditions: conditions, Mutations: mutations})
	view.Revision = revision
	return view, err
}

func (operator *GrantOperator) receipt(
	ctx context.Context, locator idempotency.IdempotencyLocator, evidence replayintent.ProtectedEvidence,
) (api.RunnerCLIGrantStatus, bool, error) {
	record, err := operator.replays.Read(ctx, locator)
	if err != nil || record == nil {
		return api.RunnerCLIGrantStatus{}, false, err
	}
	marker, err := record.Marker()
	if err != nil {
		return api.RunnerCLIGrantStatus{}, true, err
	}
	resolution, err := operator.intents.ResolveMarker(ctx, evidence, marker)
	if err != nil {
		return api.RunnerCLIGrantStatus{}, true, err
	}
	view := api.RunnerCLIGrantStatus{}
	if resolution.Response.Status == http.StatusNoContent && locator.Method == http.MethodDelete {
		return view, true, nil
	}
	if resolution.Response.Status != http.StatusOK || locator.Method != http.MethodPut || record.Revision() <= 0 {
		return api.RunnerCLIGrantStatus{}, true, errs.New(
			errs.KindInternal,
			"Runner CLI grant acceptance receipt is invalid",
		)
	}
	if err := json.Unmarshal(resolution.Response.Body, &view); err != nil {
		return api.RunnerCLIGrantStatus{}, true, errs.Wrap(errs.KindInternal, err)
	}
	view.Revision = record.Revision()
	return view, true, nil
}
