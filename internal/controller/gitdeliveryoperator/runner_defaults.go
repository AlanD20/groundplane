package gitdeliveryoperator

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/gitselector"
	"github.com/AlanD20/groundplane/internal/controller/gitdelivery"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/githubconnection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const repositoryRoute = connectionRoute + "/repositories/{repository_id}"
const runnerDefaultRoute = repositoryRoute + "/runner-default"

var _ gitdelivery.RunnerDefaultResolver = (*Operator)(nil)

// ResolveRunnerDefault returns the nearest saved default inside the binding's
// connection boundary, together with the comparisons needed for admission.
func (operator *Operator) ResolveRunnerDefault(
	ctx context.Context,
	binding gitdelivery.Binding,
) (gitselector.Selector, []keyvalue.Condition, error) {
	connection := bindingScope(binding)
	var keys []githubconnection.RunnerDefaultKey
	for _, owner := range ownerChain(binding.Target) {
		keys = append(keys, githubconnection.RunnerDefaultKey{
			Owner: owner, ConnectionID: binding.ConnectionID, RepositoryID: binding.RepositoryID,
		})
		if owner == connection {
			found, guards, err := operator.connections.ResolveRunnerDefault(ctx, keys)
			if err != nil {
				return gitselector.Selector{}, nil, err
			}
			if found.Revision == 0 {
				return gitselector.Selector{}, nil, errs.New(
					errs.KindStateConflict,
					"configure a Runner default for this repository or choose an explicit selector",
				)
			}
			return found.Record.Selector, guards, nil
		}
	}
	return gitselector.Selector{}, nil, errs.New(
		errs.KindScopeUnauthorized,
		"GitHub connection is outside the repository default owner",
	)
}

func (operator *Operator) GitRepositoryRunnerDefault(
	ctx context.Context,
	scope api.GitHubConnectionScope,
	connectionID string,
	repositoryID int64,
) (api.GitRepositoryRunnerDefaultState, error) {
	_, key, _, err := operator.repositoryTarget(ctx, scope, connectionID, repositoryID)
	if err != nil {
		return api.GitRepositoryRunnerDefaultState{}, err
	}
	current, _, err := operator.connections.ResolveRunnerDefault(ctx, []githubconnection.RunnerDefaultKey{key})
	if err != nil || current.Revision == 0 {
		return api.GitRepositoryRunnerDefaultState{}, err
	}
	return publicRunnerDefault(key, current.Record.Selector, current.Revision), nil
}

func (operator *Operator) PutGitRepositoryRunnerDefault(
	ctx context.Context,
	scope api.GitHubConnectionScope,
	connectionID string,
	repositoryID, expectedRevision int64,
	input api.GitRepositoryRunnerDefaultWrite,
	key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	selector := gitselector.Selector{Label: input.Label, Group: input.Group, GroupID: input.GroupID}
	if !selector.Valid() {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"invalid Runner label or group",
		)
	}
	return operator.changeGitRepositoryRunnerDefault(
		ctx,
		scope,
		connectionID,
		repositoryID,
		expectedRevision,
		&selector,
		key,
	)
}

func (operator *Operator) DeleteGitRepositoryRunnerDefault(
	ctx context.Context,
	scope api.GitHubConnectionScope,
	connectionID string,
	repositoryID, expectedRevision int64,
	key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return operator.changeGitRepositoryRunnerDefault(ctx, scope, connectionID, repositoryID, expectedRevision, nil, key)
}

func (operator *Operator) changeGitRepositoryRunnerDefault(
	ctx context.Context,
	scope api.GitHubConnectionScope,
	connectionID string,
	repositoryID, expectedRevision int64,
	selector *gitselector.Selector,
	key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if _, _, err := operator.resolveOwner(ctx, connectionScope(scope)); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	request := mutation{
		scope: scope.Kind, scopeID: scope.ID, method: http.MethodPut, route: runnerDefaultRoute,
		expected: expectedRevision, key: key, status: http.StatusOK, body: encodeMutationBody(selector),
		path: []requestidempotency.PathBinding{
			pathBinding("id", connectionID),
			pathBinding("repository_id", strconv.FormatInt(repositoryID, 10)),
		},
	}
	finalize := setRunnerDefaultRevision
	if selector == nil {
		request.method, request.status = http.MethodDelete, http.StatusNoContent
		finalize = nil
	}
	return operator.mutate(ctx, request, func(publish publish) (int64, error) {
		binding, resource, owner, err := operator.repositoryTarget(ctx, scope, connectionID, repositoryID)
		if err != nil {
			return 0, err
		}
		active, err := operator.connections.ActiveFence(
			ctx,
			bindingScope(binding),
			binding.ConnectionID,
			uint64(binding.ConnectionGeneration),
		)
		if err != nil {
			return 0, err
		}
		var response any
		guards := append(owner, active...)
		if selector != nil {
			binding.RunnerLabel, binding.RunnerGroup, binding.RunnerGroupID = selector.Label, selector.Group, selector.GroupID
			runners, err := operator.registeredRunners(ctx, binding)
			if err != nil {
				return 0, err
			}
			matched := false
			for _, runner := range runners {
				if runner.selectedBy(binding) {
					matched = true
					guards = append(guards, runner.conditions...)
				}
			}
			if !matched {
				return 0, errs.New(
					errs.KindStateConflict,
					"no scope-authorized Groundplane Runner matches this repository label and group",
				)
			}
			response = publicRunnerDefault(resource, *selector, 0)
		}
		conditions, effects, err := publish(response)
		if err != nil {
			return 0, err
		}
		defer keyvalue.ClearMutationValues(effects)
		guards, err = distinctConditions(append(guards, conditions...))
		if err != nil {
			return 0, err
		}
		return operator.connections.ChangeRunnerDefault(ctx, resource, selector, expectedRevision, guards, effects)
	}, finalize)
}

func (operator *Operator) repositoryTarget(
	ctx context.Context,
	scope api.GitHubConnectionScope,
	connectionID string,
	repositoryID int64,
) (gitdelivery.Binding, githubconnection.RunnerDefaultKey, []keyvalue.Condition, error) {
	owner := connectionScope(scope)
	if repositoryID <= 0 {
		return gitdelivery.Binding{}, githubconnection.RunnerDefaultKey{}, nil, errs.New(
			errs.KindValidationFailed,
			"GitHub repository ID is required",
		)
	}
	target, guards, err := operator.resolveOwner(ctx, owner)
	if err != nil {
		return gitdelivery.Binding{}, githubconnection.RunnerDefaultKey{}, nil, err
	}
	connection, err := operator.resolveConnection(ctx, target, connectionID)
	if err != nil {
		return gitdelivery.Binding{}, githubconnection.RunnerDefaultKey{}, nil, err
	}
	return gitdelivery.Binding{
		Target: target, ConnectionID: connection.ID, ConnectionGeneration: int64(connection.Generation),
		ConnectionScopeKind: connection.Scope.Kind, ConnectionScopeID: connection.Scope.ID,
		InstallationID: connection.InstallationID, RepositoryID: repositoryID,
	}, githubconnection.RunnerDefaultKey{Owner: owner, ConnectionID: connection.ID, RepositoryID: repositoryID}, guards, nil
}

func publicRunnerDefault(
	key githubconnection.RunnerDefaultKey,
	selector gitselector.Selector,
	revision int64,
) api.GitRepositoryRunnerDefaultState {
	return api.GitRepositoryRunnerDefaultState{Revision: revision, Default: &api.GitRepositoryRunnerDefault{
		Scope: publicScope(key.Owner), ConnectionID: key.ConnectionID, RepositoryID: key.RepositoryID,
		GitRepositoryRunnerDefaultWrite: api.GitRepositoryRunnerDefaultWrite{
			Label:   selector.Label,
			Group:   selector.Group,
			GroupID: selector.GroupID,
		},
	}}
}

func setRunnerDefaultRevision(response *idempotencyrecord.IdempotencyResponse, revision int64) error {
	var state api.GitRepositoryRunnerDefaultState
	if json.Unmarshal(response.Body, &state) != nil || state.Default == nil || revision <= 0 {
		return errs.New(errs.KindInternal, "repository Runner default receipt is invalid")
	}
	state.Revision = revision
	encoded, err := json.Marshal(state)
	if err != nil {
		return errs.New(errs.KindInternal, "encode GitHub repository Runner default revision")
	}
	response.Body = encoded
	return nil
}
