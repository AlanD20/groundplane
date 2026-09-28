package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/api"
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

type unusedImageResolver struct{ calls int }

func (resolver *unusedImageResolver) ResolveWorkloadImages(
	context.Context,
	string,
	[]*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	resolver.calls++
	return nil, nil
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
	return func() *imagedelivery.Service {
		service, err := imagedelivery.New(engine, store, tasks, evidence, coordinator)
		if err != nil {
			t.Fatal(err)
		}
		return service
	}
}

// IMG-02/03: removal fences stale publication, keeps an uncertain delete fenced
// across a new Controller service instance, and releases only after settlement.
func TestImageRemovalFencesSelectionsAndResumesUncertainDeletion(t *testing.T) {
	ctx := t.Context()
	store := newMemoryHierarchyStore()
	store.revision = 1
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	engine := &removalEngine{
		images:  []imagefetch.LocalImage{{ID: "sha256:" + strings.Repeat("a", 64)}},
		failure: errs.New(errs.KindInternal, "lost Docker response"),
	}
	newService := removalServiceFactory(t, store, tasks, engine)
	service := newService()
	stale := imagefence.WithScope(ctx)
	resolver := imagedelivery.SelectionResolver{Images: &unusedImageResolver{}, Store: store}
	if _, err := resolver.ResolveWorkloadImages(stale, "agent", nil); err != nil {
		t.Fatal(err)
	}
	response, err := service.RemoveImage(ctx, engine.images[0].ID, "remove-image-once")
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	claim, found, err := tasks.ClaimNextControllerTask(ctx, time.Now().UTC())
	if err != nil || !found || claim.Task.Record.ID != accepted.TaskID {
		t.Fatalf("claim: %t %v", found, err)
	}
	if err := service.Execute(ctx, claim.Task.Record); err == nil {
		t.Fatal("unknown Docker result succeeded")
	}
	if _, err := resolver.ResolveWorkloadImages(imagefence.WithScope(ctx), "agent", nil); !errors.Is(
		err,
		errs.New(errs.KindResourceInUse, ""),
	) {
		t.Fatalf("uncertain deletion did not fence new selections: %v", err)
	}
	engine.failure = nil
	service = newService()
	if err := service.Execute(ctx, claim.Task.Record); err != nil {
		t.Fatal(err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, accepted.TaskID, taskjournal.TaskStatusCompleted, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	conditions := keyvalue.ImageSelectionConditions(stale, nil)
	result, err := store.Transact(
		stale,
		conditions,
		[]keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: "/test/stale-publication", Value: []byte("must not publish")},
		},
	)
	if err != nil || result.Succeeded {
		t.Fatalf("pre-removal selection published: %#v %v", result, err)
	}
	if _, err := keyvalue.ImageSelectionResult(stale, result); !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("stale result: %v", err)
	}
	fresh := imagefence.WithScope(ctx)
	if _, err := resolver.ResolveWorkloadImages(fresh, "agent", nil); err != nil {
		t.Fatal(err)
	}
	if revision, ok := imagefence.Revision(fresh); !ok || revision <= 1 {
		t.Fatal("settlement reset the image epoch")
	}
	replayed, err := service.RemoveImage(ctx, claim.Task.Record.Target, "remove-image-once")
	if err != nil || string(replayed.Body) != string(response.Body) || engine.removals != 2 {
		t.Fatalf("removal replay: %s %v calls=%d", replayed.Body, err, engine.removals)
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
