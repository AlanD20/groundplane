package releaseoperation

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The embedded native request retains the existing accepted payload fields.
// Only Deploy accepted under scoped authorization carries the sealed source.
type acceptedServiceDeploy struct {
	release.ServiceDeployInput
	Source                *deploySource `json:"guarded_source,omitempty"`
	RemoteDesiredRevision int64         `json:"remote_desired_revision,omitempty"`
}

type deploySource struct {
	TenantRevision      int64 `json:"tenant_revision"`
	ProjectRevision     int64 `json:"project_revision"`
	EnvironmentRevision int64 `json:"environment_revision"`
	DesiredRevision     int64 `json:"desired_revision"`
	EpochRevision       int64 `json:"epoch_revision,omitempty"`
}

func captureDeploySource(
	scope workexecution.Scope,
	desiredRevision int64,
	guards []keyvalue.Condition,
) (deploySource, error) {
	var source deploySource
	tenant := hierarchy.TenantKey(scope.TenantID)
	project := hierarchy.ProjectKey(scope.ProjectID)
	environment := hierarchy.EnvironmentKey(scope.EnvironmentID)
	desired := blueprints.EnvironmentBlueprintHeadKey(scope.EnvironmentID)
	epoch := hierarchy.EnvironmentMutationEpochKey(scope.EnvironmentID)
	for _, guard := range guards {
		if guard.Prefix || guard.ModRevision <= 0 {
			continue
		}
		switch guard.Key {
		case tenant:
			source.TenantRevision = guard.ModRevision
		case project:
			source.ProjectRevision = guard.ModRevision
		case environment:
			source.EnvironmentRevision = guard.ModRevision
		case desired:
			source.DesiredRevision = guard.ModRevision
		case epoch:
			source.EpochRevision = guard.ModRevision
		}
	}
	if scope.TenantID == "" || source.TenantRevision == 0 || source.ProjectRevision == 0 ||
		source.EnvironmentRevision == 0 || source.DesiredRevision == 0 || source.DesiredRevision != desiredRevision {
		return deploySource{}, staleDeploySource()
	}
	return source, nil
}

func (source deploySource) matches(target workexecution.Scope, scope releasequeries.ReleasePlanningScope) bool {
	return target.TenantID == scope.Tenant.Record.ID && source.TenantRevision > 0 &&
		source.TenantRevision == scope.Tenant.Revision &&
		target.ProjectID == scope.Project.Record.ID &&
		source.ProjectRevision > 0 &&
		source.ProjectRevision == scope.Project.Revision &&
		target.EnvironmentID == scope.Environment.Record.ID &&
		source.EnvironmentRevision > 0 &&
		source.EnvironmentRevision == scope.Environment.Revision &&
		source.DesiredRevision > 0 &&
		source.DesiredRevision == scope.Compose.Revision &&
		(source.EpochRevision == 0 || source.EpochRevision == scope.EnvironmentEpochRevision)
}

func staleDeploySource() error {
	return errs.New(
		errs.KindStateConflict,
		"Service or Environment configuration changed after the Deploy was authorized; submit a new Deploy",
	)
}
