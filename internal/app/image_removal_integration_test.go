package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
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

// IMG-02: lack of containers must not make rollback material deletable. Use a
// real published Release, then remove the live projection from this fixture.
func TestImageRemovalProtectsRetainedReleaseWithoutContainers(t *testing.T) {
	testBlueprintExecutedArtifact(
		t,
		false,
		false,
		func(fixture *ExecutedArtifactFixture, _ *taskplanning.TaskPlanResolver, render releaserender.ReleaseRenderInput, _ domain.Intent, _ *agentpb.ComposeArtifact) {
			ctx := t.Context()
			if _, err := fixture.store.Delete(ctx, environmentprojection.EnvironmentComposeProjectionStorageKey(render.EnvironmentID)); err != nil {
				t.Fatal(err)
			}
			engine := &removalEngine{images: []imagefetch.LocalImage{{ID: render.CandidateWorkload.LocalImageID}}}
			service := removalServiceFactory(t, fixture.store.memoryHierarchyStore, fixture.Tasks, engine)()
			inventory, err := service.ListImages(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(inventory.Images) != 1 ||
				!strings.HasPrefix(inventory.Images[0].RemovalBlocked, "Retained Release ") {
				t.Fatalf("lost retained Release authority: %#v", inventory.Images)
			}
			if _, err := service.RemoveImage(ctx, engine.images[0].ID, "remove-retained-image"); !errors.Is(
				err,
				errs.New(errs.KindResourceInUse, ""),
			) ||
				engine.removals != 0 {
				t.Fatalf("retained image removal: %v calls=%d", err, engine.removals)
			}
		},
	)
}
