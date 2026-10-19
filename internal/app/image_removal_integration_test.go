package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	compose "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

type removalEngine struct {
	imageFetchRegistry
	images   []imagefetch.LocalImage
	removals int
	failure  error
}

func (engine *removalEngine) List(context.Context) ([]imagefetch.LocalImage, error) {
	return engine.images, nil
}
func (engine *removalEngine) Remove(context.Context, string) error {
	engine.removals++
	if engine.failure != nil {
		return engine.failure
	}
	engine.images = nil
	return nil
}

func removalServiceFactory(
	t *testing.T,
	store *memoryHierarchyStore,
	tasks *etcd.TaskRepository,
	engine *removalEngine,
) func() *imagedelivery.Service {
	t.Helper()
	protector, encrypted := manualJourneyEncryptedValue(t, "image-removal-intent")
	clear(encrypted)
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	diagnostics, err := executiondiagnostics.New(store, queue)
	if err != nil {
		t.Fatal(err)
	}
	releases, err := etcd.NewReleaseLedger(store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	return func() *imagedelivery.Service {
		service, err := imagedelivery.New(engine, store, tasks, evidence, coordinator, diagnostics, releases)
		if err != nil {
			t.Fatal(err)
		}
		return service
	}
}

// IMG-02: retained exact Release input protects its image without a live
// projection, container or Task journal. This stages real rendered input; it
// does not claim a completed host deployment or rollback qualification.
func TestImageRemovalProtectsRetainedReleaseWithoutContainers(t *testing.T) {
	f := newNativeImageFixture(t)
	at := time.Now().UTC()
	tenantID, projectID, environmentID := ids.New(
		ids.KindTenant,
	), ids.New(
		ids.KindProject,
	), ids.New(
		ids.KindEnvironment,
	)
	serviceID, releaseID, planID := ids.New(ids.KindService), ids.New(ids.KindDeployment), ids.New(ids.KindPlan)
	artifactID, taskID, operationID := ids.New(ids.KindConfig), ids.New(ids.KindTask), ids.New(ids.KindOperation)
	imageID := "sha256:" + strings.Repeat("a", 64)
	project := &compose.Project{Name: "retention", Services: compose.Services{"api": {
		Name: "api", Image: "example/api:retained", NetworkMode: "none",
	}}}
	normalized, err := composerender.MarshalNormalizedEnvironmentProject(project)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: project, ArtifactID: artifactID, ProjectOwnerKind: composerender.ComposeProjectOwnerTenant,
		TenantID: tenantID, ProjectID: projectID, EnvironmentID: environmentID, PlanID: planID, RenderGeneration: 1,
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/" + environmentID,
		Identities: composeidentity.Snapshot{
			Services: []composeidentity.Resource{{ID: serviceID, Name: "api"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	workload := domain.WorkloadSeal{RequestedReference: "example/api:retained", LocalImageID: imageID, ReplicaCount: 1}
	projection := environmentprojection.EnvironmentComposeProjection{EnvironmentID: environmentID, RevisionID: taskID,
		RenderGeneration: 1, NormalizedCompose: normalized, ComposeArtifact: encoded,
		DesiredServices: []services.EnvironmentServiceProjection{{EnvironmentID: environmentID,
			Desired: core.Service{
				ID:       serviceID,
				Name:     "api",
				Image:    workload.RequestedReference,
				Replicas: 1,
				Strategy: core.StrategyRecreate,
			}}}}
	render := releaserender.ReleaseRenderInput{
		ReleaseID:           releaseID,
		PlanID:              planID,
		ArtifactID:          artifactID,
		ServiceID:           serviceID,
		ServiceName:         "api",
		CandidateWorkload:   workload,
		Strategy:            domain.StrategyRecreate,
		PriorStrategy:       domain.StrategyRecreate,
		CandidateTarget:     domain.WorkloadSingleton,
		PriorTarget:         domain.WorkloadSingleton,
		TenantID:            tenantID,
		TenantSlug:          "tenant",
		ProjectID:           projectID,
		ProjectSlug:         "project",
		EnvironmentID:       environmentID,
		EnvironmentName:     "retention",
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/" + environmentID,
		Projection:          projection,
	}
	raw, err := releaserender.EncodeReleaseRenderInput(render)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := domain.Digest(json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}
	intent := domain.Intent{ID: releaseID, EnvironmentID: environmentID, ServiceID: serviceID, OperationID: operationID,
		OperationKind: domain.OperationDeploy, CandidateWorkload: workload, Tag: "retained", Strategy: domain.StrategyRecreate,
		OnFailure: domain.OnFailureSwitchBack, RenderInputID: artifactID, RenderInputDigest: digest, CreatedAt: at, Actor: "operator", OriginatingTaskID: taskID,
		Workspace: domain.Workspace{
			Kind:          domain.WorkspaceTenant,
			TenantID:      tenantID,
			ProjectID:     projectID,
			EnvironmentID: environmentID,
		}}
	tasks, err := etcd.NewTaskRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := etcd.NewReleaseLedger(f.store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Stage(t.Context(), releases.ReleaseStage{PublicationID: ids.NewULID(), OperationID: operationID, CreatedAt: at,
		Members: []releases.ReleaseStageMember{{Intent: intent, RenderInput: raw,
			Checkpoint: domain.Checkpoint{ReleaseID: releaseID, State: domain.StatePending, UpdatedAt: at}}}}); err != nil {
		t.Fatal(err)
	}
	f.registry.images = []imagefetch.LocalImage{{ID: imageID}}
	inventory, err := f.service.ListImages(t.Context())
	if err != nil || len(inventory.Images) != 1 || inventory.Images[0].RemovalBlocked != "Retained Release "+releaseID {
		t.Fatalf("retained Release lost image protection without runtime: %#v %v", inventory, err)
	}
	if _, err := f.service.RemoveImage(t.Context(), imageID, "remove-retained-image"); !errors.Is(
		err,
		errs.New(errs.KindResourceInUse, ""),
	) ||
		f.registry.removals != 0 {
		t.Fatalf("retained image removal: %v calls=%d", err, f.registry.removals)
	}
}
