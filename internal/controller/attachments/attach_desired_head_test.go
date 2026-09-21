package attachments

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

// Rationale: direct desired-state mutations publish a selected Compose projection without a Blueprint audit input;
// Attach must consume that selected projection instead of requiring a historical Blueprint document.
func TestResolveAttachScopeAcceptsSelectedDesiredProjectionWithoutBlueprintAudit(t *testing.T) {
	registerAdapters()
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

	consumer := testkeyvalue.Versioned[testservices.ServiceRecord]{
		Record: testservices.ServiceRecord{
			EnvironmentID: environmentID,
			Desired:       core.Service{ID: consumerID, Name: "api"},
			Runtime: core.ServiceRuntime{
				ServiceID: consumerID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
			},
		},
		Revision: 11,
	}
	backing := testkeyvalue.Versioned[testservices.ServiceRecord]{
		Record: testservices.ServiceRecord{
			EnvironmentID: backingEnvironmentID, BackingNetworkID: backingNetworkID,
			Desired: core.Service{ID: backingServiceID, Name: "postgres", Adapter: "custom"},
			Runtime: core.ServiceRuntime{
				ServiceID: backingServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
			},
		},
		Revision: 12,
	}
	repository := &attachSelectedDesiredRepository{
		tenant: testkeyvalue.Versioned[testhierarchy.TenantRecord]{
			Record: testhierarchy.TenantRecord{ID: tenantID, Slug: "tenant"}, Revision: 1,
		},
		project: testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
			Record: testhierarchy.ProjectRecord{
				ID: projectID, TenantID: tenantID, Slug: "project", Kind: testhierarchy.ProjectKindTenant,
			},
			Revision: 2,
		},
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{
				ID: environmentID, ProjectID: projectID, Name: "production",
				ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
			},
			Revision: 3,
		},
		consumer: consumer,
		backingProject: testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
			Record: testhierarchy.ProjectRecord{
				ID: backingProjectID, Slug: "postgres", Kind: testhierarchy.ProjectKindBacking,
			},
			Revision: 4,
		},
		backingEnvironment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{
				ID: backingEnvironmentID, ProjectID: backingProjectID, Name: "main",
				ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
			},
			Revision: 5,
		},
		backing: backing,
		head: testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead]{
			Record: testblueprints.EnvironmentBlueprintHead{
				EnvironmentID: environmentID, RevisionID: desiredRevisionID,
			},
			Revision: 13,
		},
		projection: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record: testenvironmentprojection.EnvironmentComposeProjection{
				EnvironmentID: environmentID, RevisionID: desiredRevisionID, RenderGeneration: 2,
				DesiredServices: []testservices.EnvironmentServiceProjection{{
					EnvironmentID: environmentID, Desired: consumer.Record.Desired,
				}},
			},
			Revision: 14,
		},
	}
	service := &MutationService{repository: repository}

	scope, attaches, adapter, err := service.resolveAttachScope(
		context.Background(), consumer, backingServiceID, "", nil,
	)
	if err != nil {
		t.Fatalf("resolveAttachScope() error = %v", err)
	}
	if scope.ComposeProjection.Record.RevisionID != desiredRevisionID || adapter.Key() != "custom" ||
		len(attaches) != 0 {
		t.Fatalf("resolveAttachScope() = %#v/%q/%#v", scope.ComposeProjection, adapter.Key(), attaches)
	}
}

type attachSelectedDesiredRepository struct {
	attachMutationRepository
	tenant             testkeyvalue.Versioned[testhierarchy.TenantRecord]
	project            testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	environment        testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	consumer           testkeyvalue.Versioned[testservices.ServiceRecord]
	backingProject     testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	backingEnvironment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	backing            testkeyvalue.Versioned[testservices.ServiceRecord]
	head               testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead]
	projection         testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]
}

func (repository *attachSelectedDesiredRepository) GetTenant(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.TenantRecord], error) {
	return repository.tenant, nil
}

func (repository *attachSelectedDesiredRepository) GetProject(
	_ context.Context,
	id string,
) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	if id == repository.project.Record.ID {
		return repository.project, nil
	}
	return repository.backingProject, nil
}

func (repository *attachSelectedDesiredRepository) GetEnvironment(
	_ context.Context,
	id string,
) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	if id == repository.environment.Record.ID {
		return repository.environment, nil
	}
	return repository.backingEnvironment, nil
}

func (repository *attachSelectedDesiredRepository) GetService(
	_ context.Context,
	id string,
) (testkeyvalue.Versioned[testservices.ServiceRecord], error) {
	if id == repository.consumer.Record.Desired.ID {
		return repository.consumer, nil
	}
	return repository.backing, nil
}

func (repository *attachSelectedDesiredRepository) GetEnvironmentBlueprintHead(
	context.Context,
	string,
) (testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead], bool, error) {
	return repository.head, true, nil
}

func (*attachSelectedDesiredRepository) GetEnvironmentBlueprintRevision(
	context.Context,
	string,
	string,
) (testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision], bool, error) {
	return testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision]{}, false, nil
}

func (repository *attachSelectedDesiredRepository) GetEnvironmentComposeProjection(
	context.Context,
	string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	return repository.projection, true, nil
}

func (repository *attachSelectedDesiredRepository) GetEnvironmentComposeProjectionRevision(
	_ context.Context,
	_ string,
	revisionID string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	return repository.projection, revisionID == repository.projection.Record.RevisionID, nil
}

func (*attachSelectedDesiredRepository) ListAttaches(
	context.Context,
	string, testkeyvalue.PageRequest,

) (testkeyvalue.Page[testattachments.Record], error) {
	return testkeyvalue.Page[testattachments.Record]{Revision: 15}, nil
}
