package gitdeliveryoperator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/gitdelivery"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const serviceBindingRoute = "/services/{id}/git-deployment"
const serviceDeploymentsRoute = "/services/{id}/git-deployments"

func (operator *Operator) GitDeploymentBinding(
	ctx context.Context,
	serviceID string,
) (api.GitDeploymentBindingState, error) {
	target, _, err := operator.resolveTarget(ctx, serviceID)
	if err != nil {
		return api.GitDeploymentBindingState{}, err
	}
	snapshot, err := operator.delivery.GetBinding(ctx, serviceID)
	if err != nil {
		if kind, _ := errs.KindOf(err); kind == errs.KindRequestNotFound {
			return api.GitDeploymentBindingState{}, nil
		}
		return api.GitDeploymentBindingState{}, err
	}
	if snapshot.Binding.Target != target {
		return api.GitDeploymentBindingState{}, errs.New(
			errs.KindScopeUnauthorized,
			"GitHub binding is outside the Service owner",
		)
	}
	projected := publicBinding(snapshot.Binding)
	return api.GitDeploymentBindingState{Binding: &projected, Revision: snapshot.Revision}, nil
}

func (operator *Operator) PutGitDeploymentBinding(
	ctx context.Context,
	serviceID string,
	revision int64,
	input api.GitDeploymentBindingWrite,
	key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	target, _, err := operator.resolveTarget(ctx, serviceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return operator.mutate(ctx, mutation{
		scope: "environment", scopeID: target.EnvironmentID, method: http.MethodPut, route: serviceBindingRoute, key: key,
		path: []requestidempotency.PathBinding{
			pathBinding("id", serviceID),
		}, expected: revision, body: encodeMutationBody(input), status: http.StatusOK,
	}, func(publish publish) (int64, error) {
		connection, err := operator.resolveConnection(ctx, target, input.ConnectionID)
		if err != nil {
			return 0, err
		}
		binding := gitdelivery.Binding{
			Target: target, ConnectionID: connection.ID, ConnectionGeneration: int64(connection.Generation),
			ConnectionScopeKind: connection.Scope.Kind, ConnectionScopeID: connection.Scope.ID,
			InstallationID: connection.InstallationID, RepositoryID: input.RepositoryID, WorkflowID: input.WorkflowID,
			WorkflowRef: input.WorkflowRef, WorkflowCommit: input.WorkflowCommit, Branch: input.Branch,
			Dockerfile: input.Dockerfile, BuildContext: input.BuildContext, RunnerLabel: input.RunnerLabel, InheritRunnerDefault: input.InheritRunnerDefault,
			RunnerGroup: input.RunnerGroup, RunnerGroupID: input.RunnerGroupID, Automatic: input.Automatic, RequireApproval: input.RequireApproval,
		}
		if binding.Automatic && binding.Branch == binding.WorkflowRef {
			return 0, errs.New(
				errs.KindValidationFailed,
				"automatic deployment requires a separate protected branch for the approved GitHub workflow",
			)
		}
		if binding.WorkflowCommit == "" {
			binding.WorkflowCommit, err = operator.resolveBranch(ctx, binding, binding.WorkflowRef)
			if err != nil {
				return 0, err
			}
		}
		snapshot, err := operator.delivery.Bind(
			ctx,
			binding,
			revision,
			func(binding gitdelivery.Binding) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
				projected := publicBinding(binding)
				return publish(api.GitDeploymentBindingState{Binding: &projected})
			},
		)
		return snapshot.Revision, err
	}, setBindingRevision)
}

func setBindingRevision(response *idempotencyrecord.IdempotencyResponse, revision int64) error {
	var state api.GitDeploymentBindingState
	if json.Unmarshal(response.Body, &state) != nil || state.Binding == nil || revision <= 0 {
		return errs.New(errs.KindInternal, "GitHub binding acceptance receipt is invalid")
	}
	state.Revision = revision
	body, err := json.Marshal(state)
	if err != nil {
		return errs.New(errs.KindInternal, "encode GitHub binding revision")
	}
	response.Body = body
	return nil
}

func (operator *Operator) ListGitDeployments(
	ctx context.Context,
	serviceID string,
	request keyvalue.PageRequest,
) (api.Page[api.GitDeployment], error) {
	target, _, err := operator.resolveTarget(ctx, serviceID)
	if err != nil {
		return api.Page[api.GitDeployment]{}, err
	}
	if request.Limit > 64 {
		return api.Page[api.GitDeployment]{}, errs.New(
			errs.KindValidationFailed,
			"GitHub deployment page limit cannot exceed 64",
		)
	}
	limit, revision, after, query, err := recordquery.NormalizePageRequest(
		request.NewestFirst(), "git-deployments", "service", serviceID, "", ids.KindOperation,
	)
	if err != nil {
		return api.Page[api.GitDeployment]{}, err
	}
	page, err := operator.delivery.List(ctx, serviceID, after, int64(limit), revision)
	if err != nil {
		return api.Page[api.GitDeployment]{}, err
	}
	result := api.Page[api.GitDeployment]{Items: make([]api.GitDeployment, 0, len(page.Items)), Revision: page.Revision}
	for _, run := range page.Items {
		if run.Binding.Target != target {
			return api.Page[api.GitDeployment]{}, errs.New(
				errs.KindScopeUnauthorized,
				"GitHub deployment is outside the Service owner",
			)
		}
		result.Items = append(result.Items, publicRun(run))
	}
	if page.Next != "" {
		result.NextCursor, err = recordcodec.EncodeCursor(recordcodec.Cursor{
			Version: recordcodec.CursorVersion, Query: query, LastID: page.Next, Revision: page.Revision,
		})
	}
	return result, err
}

func (operator *Operator) StartGitDeployment(
	ctx context.Context,
	serviceID string,
	input api.GitDeploymentStartRequest,
	key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	target, targetFence, err := operator.resolveTarget(ctx, serviceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return operator.mutate(ctx, mutation{
		scope: "environment", scopeID: target.EnvironmentID, method: http.MethodPost, route: serviceDeploymentsRoute, key: key,
		path: []requestidempotency.PathBinding{
			pathBinding("id", serviceID),
		}, body: encodeMutationBody(input), status: http.StatusAccepted,
	}, func(publish publish) (int64, error) {
		digest := sha256.Sum256([]byte(key))
		newReceipt := false
		run, err := operator.delivery.Begin(ctx, serviceID, gitdelivery.StartInput{
			ExpectedCommit: input.ExpectedCommit, IdempotencyKey: "operator-" + hex.EncodeToString(digest[:]),
		}, func(run gitdelivery.Run) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
			newReceipt = true
			return publish(publicRun(run))
		})
		if err != nil || newReceipt {
			return 0, err
		}
		// The native request survives the HTTP marker retention window. Rebuild
		// the original receipt from immutable admission fields without a new run.
		receipt := api.GitDeployment{
			ID: run.ID, Binding: publicBinding(run.Binding), BindingRevision: run.BindingRevision,
			Commit: run.Commit, Phase: string(gitdelivery.Queued), CreatedAt: run.CreatedAt, UpdatedAt: run.CreatedAt,
		}
		conditions, mutations, err := publish(receipt)
		if err != nil {
			return 0, err
		}
		defer keyvalue.ClearMutationValues(mutations)
		result, err := operator.store.Transact(ctx, append(conditions, targetFence...), mutations)
		if err != nil {
			return 0, err
		}
		if !result.Succeeded {
			return 0, errs.New(errs.KindStateConflict, "GitHub deployment receipt changed")
		}
		return result.Revision, nil
	}, nil)
}

func (operator *Operator) GitDeployment(
	ctx context.Context,
	serviceID, deploymentID string,
) (api.GitDeployment, error) {
	target, _, err := operator.resolveTarget(ctx, serviceID)
	if err != nil {
		return api.GitDeployment{}, err
	}
	run, err := operator.delivery.Get(ctx, serviceID, deploymentID)
	if err != nil {
		return api.GitDeployment{}, err
	}
	if run.Binding.Target != target {
		return api.GitDeployment{}, errs.New(
			errs.KindScopeUnauthorized,
			"GitHub deployment is outside the Service owner",
		)
	}
	return publicRun(run), nil
}

func (operator *Operator) ApproveGitDeployment(
	ctx context.Context,
	serviceID, deploymentID, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	target, _, err := operator.resolveTarget(ctx, serviceID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return operator.mutate(ctx, mutation{
		scope: "environment", scopeID: target.EnvironmentID, method: http.MethodPost, route: serviceDeploymentsRoute + "/{deployment}/approve", key: key,
		path:   []requestidempotency.PathBinding{pathBinding("id", serviceID), pathBinding("deployment", deploymentID)},
		status: http.StatusAccepted,
	}, func(publish publish) (int64, error) {
		_, err := operator.delivery.Approve(ctx, serviceID, deploymentID, runReceipt(publish))
		return 0, err
	}, nil)
}

func runReceipt(publish publish) gitdelivery.RunEffects {
	return func(run gitdelivery.Run) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
		return publish(publicRun(run))
	}
}

func publicBinding(binding gitdelivery.Binding) api.GitDeploymentBinding {
	return api.GitDeploymentBinding{
		ServiceID: binding.ServiceID, EnvironmentID: binding.EnvironmentID, ProjectID: binding.ProjectID, TenantID: binding.TenantID,
		GitDeploymentBindingWrite: api.GitDeploymentBindingWrite{
			ConnectionID: binding.ConnectionID, RepositoryID: binding.RepositoryID, WorkflowID: binding.WorkflowID,
			WorkflowRef: binding.WorkflowRef, WorkflowCommit: binding.WorkflowCommit, Branch: binding.Branch,
			Dockerfile: binding.Dockerfile, BuildContext: binding.BuildContext, RunnerLabel: binding.RunnerLabel, InheritRunnerDefault: binding.InheritRunnerDefault,
			RunnerGroup: binding.RunnerGroup, RunnerGroupID: binding.RunnerGroupID, Automatic: binding.Automatic, RequireApproval: binding.RequireApproval,
		},
	}
}

func publicRun(run gitdelivery.Run) api.GitDeployment {
	return api.GitDeployment{
		ID: run.ID, Binding: publicBinding(run.Binding), BindingRevision: run.BindingRevision, Commit: run.Commit,
		Phase: string(
			run.Phase,
		), GitHubRunID: run.GitHubRunID, GitHubRunAttempt: run.GitHubRunAttempt, GitHubURL: run.GitHubURL,
		GitHubRunnerID: run.GitHubRunnerID, ImageDigest: run.ImageDigest, FetchTaskID: run.FetchTaskID, DeployTaskID: run.DeployTaskID,
		ReleaseID: run.ReleaseID, ErrorCode: run.ErrorCode, CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt,
	}
}
