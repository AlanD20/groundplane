package app

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters/manual"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: direct desired-state mutations publish a selected Compose projection without a Blueprint audit input;
// Attach must consume that selected projection instead of requiring a historical Blueprint document.
func TestResolveAttachScopeAcceptsSelectedDesiredProjectionWithoutBlueprintAudit(t *testing.T) {
	manual.Register()
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 1)
	projectID := ids.NewAt(ids.KindProject, now, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 3)
	consumerID := ids.NewAt(ids.KindService, now, 4)
	backingProjectID := ids.NewAt(ids.KindProject, now, 5)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, now, 6)
	backingServiceID := ids.NewAt(ids.KindService, now, 7)
	backingNetworkID := ids.NewAt(ids.KindNetwork, now, 8)
	desiredRevisionID := ids.NewAt(ids.KindTask, now, 9)

	consumer := etcd.Versioned[etcd.ServiceRecord]{
		Record: etcd.ServiceRecord{
			EnvironmentID: environmentID,
			Desired:       core.Service{ID: consumerID, Name: "api"},
			Runtime: core.ServiceRuntime{
				ServiceID: consumerID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
			},
		},
		Revision: 11,
	}
	backing := etcd.Versioned[etcd.ServiceRecord]{
		Record: etcd.ServiceRecord{
			EnvironmentID: backingEnvironmentID, BackingNetworkID: backingNetworkID,
			Desired: core.Service{ID: backingServiceID, Name: "postgres", Adapter: "manual"},
			Runtime: core.ServiceRuntime{
				ServiceID: backingServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
			},
		},
		Revision: 12,
	}
	repository := &attachSelectedDesiredRepository{
		tenant: etcd.Versioned[etcd.TenantRecord]{
			Record: etcd.TenantRecord{ID: tenantID, Slug: "tenant"}, Revision: 1,
		},
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record: etcd.ProjectRecord{
				ID: projectID, TenantID: tenantID, Slug: "project", Kind: etcd.ProjectKindTenant,
			},
			Revision: 2,
		},
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{
				ID: environmentID, ProjectID: projectID, Name: "production",
				ProvisioningState: etcd.EnvironmentProvisioningReady,
			},
			Revision: 3,
		},
		consumer: consumer,
		backingProject: etcd.Versioned[etcd.ProjectRecord]{
			Record: etcd.ProjectRecord{
				ID: backingProjectID, Slug: "postgres", Kind: etcd.ProjectKindBacking,
			},
			Revision: 4,
		},
		backingEnvironment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{
				ID: backingEnvironmentID, ProjectID: backingProjectID, Name: "main",
				ProvisioningState: etcd.EnvironmentProvisioningReady,
			},
			Revision: 5,
		},
		backing: backing,
		head: etcd.Versioned[etcd.EnvironmentBlueprintHead]{
			Record: etcd.EnvironmentBlueprintHead{
				EnvironmentID: environmentID, RevisionID: desiredRevisionID,
			},
			Revision: 13,
		},
		projection: etcd.Versioned[etcd.EnvironmentComposeProjection]{
			Record: etcd.EnvironmentComposeProjection{
				EnvironmentID: environmentID, RevisionID: desiredRevisionID, RenderGeneration: 2,
				DesiredServices: []etcd.EnvironmentServiceProjection{{
					EnvironmentID: environmentID, Desired: consumer.Record.Desired,
				}},
			},
			Revision: 14,
		},
	}
	service := &attachMutationService{repository: repository}

	scope, attaches, adapter, err := service.resolveAttachScope(
		context.Background(), consumer, backingServiceID, "", nil,
	)
	if err != nil {
		t.Fatalf("resolveAttachScope() error = %v", err)
	}
	if scope.ComposeProjection.Record.RevisionID != desiredRevisionID || adapter.Key() != "manual" ||
		len(attaches) != 0 {
		t.Fatalf("resolveAttachScope() = %#v/%q/%#v", scope.ComposeProjection, adapter.Key(), attaches)
	}
}

type attachSelectedDesiredRepository struct {
	attachMutationRepository
	tenant             etcd.Versioned[etcd.TenantRecord]
	project            etcd.Versioned[etcd.ProjectRecord]
	environment        etcd.Versioned[etcd.EnvironmentRecord]
	consumer           etcd.Versioned[etcd.ServiceRecord]
	backingProject     etcd.Versioned[etcd.ProjectRecord]
	backingEnvironment etcd.Versioned[etcd.EnvironmentRecord]
	backing            etcd.Versioned[etcd.ServiceRecord]
	head               etcd.Versioned[etcd.EnvironmentBlueprintHead]
	projection         etcd.Versioned[etcd.EnvironmentComposeProjection]
}

func (repository *attachSelectedDesiredRepository) GetTenant(
	context.Context,
	string,
) (etcd.Versioned[etcd.TenantRecord], error) {
	return repository.tenant, nil
}

func (repository *attachSelectedDesiredRepository) GetProject(
	_ context.Context,
	id string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	if id == repository.project.Record.ID {
		return repository.project, nil
	}
	return repository.backingProject, nil
}

func (repository *attachSelectedDesiredRepository) GetEnvironment(
	_ context.Context,
	id string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	if id == repository.environment.Record.ID {
		return repository.environment, nil
	}
	return repository.backingEnvironment, nil
}

func (repository *attachSelectedDesiredRepository) GetService(
	_ context.Context,
	id string,
) (etcd.Versioned[etcd.ServiceRecord], error) {
	if id == repository.consumer.Record.Desired.ID {
		return repository.consumer, nil
	}
	return repository.backing, nil
}

func (repository *attachSelectedDesiredRepository) GetEnvironmentBlueprintHead(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentBlueprintHead], bool, error) {
	return repository.head, true, nil
}

func (*attachSelectedDesiredRepository) GetEnvironmentBlueprintRevision(
	context.Context,
	string,
	string,
) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error) {
	return etcd.Versioned[etcd.EnvironmentBlueprintRevision]{}, false, nil
}

func (repository *attachSelectedDesiredRepository) GetEnvironmentComposeProjection(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.projection, true, nil
}

func (repository *attachSelectedDesiredRepository) GetEnvironmentComposeProjectionRevision(
	_ context.Context,
	_ string,
	revisionID string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.projection, revisionID == repository.projection.Record.RevisionID, nil
}

func (*attachSelectedDesiredRepository) ListAttaches(
	context.Context,
	string,
	etcd.PageRequest,
) (etcd.Page[etcd.AttachRecord], error) {
	return etcd.Page[etcd.AttachRecord]{Revision: 15}, nil
}
