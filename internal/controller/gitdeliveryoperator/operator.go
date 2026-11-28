// Package gitdeliveryoperator composes scoped GitHub delivery and the native release path.
package gitdeliveryoperator

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/gitdelivery"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/githubconnection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RunnerAuthority supplies identity and admission fences from the proven scoped Runner lifecycle.
type RunnerAuthority interface {
	GitHubDeliveryAuthority(ctx context.Context, runnerID string) (RunnerAuthorityEvidence, error)
}

type RunnerAuthorityEvidence struct {
	GitHubID   int64
	Conditions []keyvalue.Condition
}

type Operator struct {
	store       keyvalue.Store
	authority   RunnerAuthority
	hierarchy   *hierarchy.Reader
	services    *environmentqueries.ServiceReader
	runners     *runners.Reader
	connections *githubconnection.Repository
	delivery    *gitdelivery.Service
	idempotency *requestidempotency.Coordinator
	replays     *etcd.IdempotencyRepository
}

var _ gitdelivery.GitHub = (*Operator)(nil)
var _ gitdelivery.Access = (*Operator)(nil)

func New(
	store keyvalue.Store,
	connections *githubconnection.Repository,
	repository *gitdelivery.Repository,
	idempotency *requestidempotency.Coordinator,
	replays *etcd.IdempotencyRepository,
	releases gitdelivery.Releases,
	authorities ...RunnerAuthority,
) (*Operator, error) {
	if store == nil || connections == nil || repository == nil ||
		idempotency == nil || replays == nil || len(authorities) > 1 {
		return nil, errs.New(errs.KindInternal, "GitHub deployment dependencies are incomplete")
	}
	operator := &Operator{
		store: store, hierarchy: hierarchy.NewReader(store), services: environmentqueries.NewServiceReader(store),
		runners: runners.NewReader(store), connections: connections, idempotency: idempotency, replays: replays,
	}
	if len(authorities) == 1 {
		operator.authority = authorities[0]
	}
	delivery, err := gitdelivery.NewService(repository, operator, operator, releases)
	if err != nil {
		return nil, err
	}
	operator.delivery = delivery
	return operator, nil
}

func (operator *Operator) Run(ctx context.Context, interval time.Duration, logger *slog.Logger) error {
	return operator.delivery.RunAll(ctx, interval, logger)
}

func (operator *Operator) resolveTarget(
	ctx context.Context,
	serviceID string,
) (gitdelivery.Target, []keyvalue.Condition, error) {
	service, err := operator.services.GetService(ctx, serviceID)
	if err != nil {
		return gitdelivery.Target{}, nil, err
	}
	if service.Record.Desired.Adapter != "" {
		return gitdelivery.Target{}, nil, errs.New(errs.KindServiceNotFound, "Service was not found")
	}
	target, conditions, err := operator.resolveOwner(
		ctx,
		githubconnection.Scope{Kind: ids.KindEnvironment, ID: service.Record.EnvironmentID},
	)
	if err != nil {
		return gitdelivery.Target{}, nil, err
	}
	target.ServiceID = serviceID
	conditions = append(
		conditions,
		services.ServiceDesiredCondition(service),
		absentTombstone(deletions.DeletionTargetService, serviceID),
	)
	tombstone, err := operator.store.Get(ctx, conditions[len(conditions)-1].Key)
	if err != nil {
		return gitdelivery.Target{}, nil, err
	}
	if tombstone == nil || tombstone.Entry != nil {
		return gitdelivery.Target{}, nil, errs.New(errs.KindResourceInUse, "Service removal is in progress")
	}
	return target, conditions, nil
}

func (operator *Operator) resolveOwner(
	ctx context.Context,
	scope githubconnection.Scope,
) (gitdelivery.Target, []keyvalue.Condition, error) {
	var owner gitdelivery.Target
	var conditions []keyvalue.Condition
	switch scope.Kind {
	case ids.KindEnvironment:
		environment, err := operator.hierarchy.GetEnvironment(ctx, scope.ID)
		if err != nil {
			return owner, nil, err
		}
		if environment.Record.DeletionTaskID != "" {
			return owner, nil, ownerDeleting()
		}
		owner.EnvironmentID = scope.ID
		conditions = append(conditions,
			keyvalue.Condition{Key: hierarchy.EnvironmentKey(scope.ID), ModRevision: environment.Revision},
			absentTombstone(deletions.DeletionTargetEnvironment, scope.ID),
		)
		scope = githubconnection.Scope{Kind: ids.KindProject, ID: environment.Record.ProjectID}
	case ids.KindProject, ids.KindTenant:
	default:
		return owner, nil, errs.New(errs.KindValidationFailed, "GitHub connection owner is invalid")
	}
	if scope.Kind == ids.KindProject {
		project, err := operator.hierarchy.GetProject(ctx, scope.ID)
		if err != nil {
			return owner, nil, err
		}
		if project.Record.DeletionTaskID != "" {
			return owner, nil, ownerDeleting()
		}
		if project.Record.Kind != hierarchy.ProjectKindTenant {
			return owner, nil, errs.New(errs.KindScopeUnauthorized, "GitHub delivery requires a tenant-owned project")
		}
		owner.ProjectID = scope.ID
		conditions = append(conditions,
			keyvalue.Condition{Key: hierarchy.ProjectKey(scope.ID), ModRevision: project.Revision},
			absentTombstone(deletions.DeletionTargetProject, scope.ID),
		)
		scope = githubconnection.Scope{Kind: ids.KindTenant, ID: project.Record.TenantID}
	}
	tenant, err := operator.hierarchy.GetTenant(ctx, scope.ID)
	if err != nil {
		return owner, nil, err
	}
	if tenant.Record.DeletionTaskID != "" {
		return owner, nil, ownerDeleting()
	}
	owner.TenantID = scope.ID
	return owner, append(conditions,
		keyvalue.Condition{Key: hierarchy.TenantKey(scope.ID), ModRevision: tenant.Revision},
		absentTombstone(deletions.DeletionTargetTenant, scope.ID),
	), nil
}

func absentTombstone(kind deletions.DeletionTargetKind, id string) keyvalue.Condition {
	return keyvalue.Condition{Key: deletions.TombstoneKey(string(kind), id)}
}

func ownerDeleting() error {
	return errs.New(errs.KindResourceInUse, "GitHub delivery owner removal is in progress")
}

func publicScope(scope githubconnection.Scope) api.GitHubConnectionScope {
	var kind string
	switch scope.Kind {
	case ids.KindTenant:
		kind = "tenant"
	case ids.KindProject:
		kind = "project"
	case ids.KindEnvironment:
		kind = "environment"
	}
	return api.GitHubConnectionScope{Kind: kind, ID: scope.ID}
}

func connectionScope(scope api.GitHubConnectionScope) githubconnection.Scope {
	var kind ids.Kind
	switch scope.Kind {
	case "tenant":
		kind = ids.KindTenant
	case "project":
		kind = ids.KindProject
	case "environment":
		kind = ids.KindEnvironment
	}
	return githubconnection.Scope{Kind: kind, ID: scope.ID}
}

func (operator *Operator) resolveConnection(
	ctx context.Context,
	target gitdelivery.Target,
	id string,
) (githubconnection.Connection, error) {
	for _, scope := range ownerChain(target) {
		connection, err := operator.connections.Get(ctx, scope, id)
		if err == nil {
			if connection.Record.RevokedAt != nil {
				return githubconnection.Connection{}, errs.New(
					errs.KindScopeUnauthorized,
					"GitHub connection is revoked",
				)
			}
			return connection.Record, nil
		}
		kind, _ := errs.KindOf(err)
		if kind != errs.KindRequestNotFound {
			return githubconnection.Connection{}, err
		}
	}
	return githubconnection.Connection{}, errs.New(
		errs.KindRequestNotFound,
		"GitHub connection is not available to this Service",
	)
}

func ownerChain(target gitdelivery.Target) []githubconnection.Scope {
	var scopes []githubconnection.Scope
	if target.EnvironmentID != "" {
		scopes = append(scopes, githubconnection.Scope{Kind: ids.KindEnvironment, ID: target.EnvironmentID})
	}
	if target.ProjectID != "" {
		scopes = append(scopes, githubconnection.Scope{Kind: ids.KindProject, ID: target.ProjectID})
	}
	if target.TenantID != "" {
		scopes = append(scopes, githubconnection.Scope{Kind: ids.KindTenant, ID: target.TenantID})
	}
	return scopes
}
