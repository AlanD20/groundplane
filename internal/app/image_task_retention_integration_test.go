package app

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// IMG-02: failed DNS work must protect its candidate and rollback images, not
// unrelated images. Missing immutable evidence must never permit deletion.
func TestImageRemovalRetainsTerminalComponentImages(t *testing.T) {
	store := newMemoryHierarchyStore()
	store.revision = 1
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), PlanID: ids.New(ids.KindPlan),
		PlanHash: strings.Repeat("a", 64), RenderGeneration: 1,
		Owner: taskjournal.PlatformTaskOwner(), Actor: taskjournal.TaskActorSystem, Executor: taskjournal.TaskExecutorAgent,
		Type: taskjournal.TaskUpdate, Target: ids.New(ids.KindComponent),
		Params: map[string]string{taskjournal.TaskResourceKindParam: taskjournal.TaskResourceComponent},
		Status: taskjournal.TaskStatusPending, TimeoutSeconds: 300, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	input := platformcomponents.PlatformComponentTaskRenderInput{
		PlanID: task.PlanID, TaskID: task.ID, ComponentID: task.Target, DesiredSHA256: task.PlanHash,
		BaselineGeneration: 1, BaselineSHA256: task.PlanHash, HostResolutionInputRevision: 1, HostResolutionSHA256: task.PlanHash,
		Config: core.CoreDNSComponentConfig{UpstreamAuto: true}, GeneratedServiceID: ids.New(ids.KindService),
		DefinitionSHA256: task.PlanHash, CatalogSHA256: task.PlanHash, ActionID: "activate-config",
		ArtifactID: ids.New(
			ids.KindConfig,
		), ComposeArtifactID: ids.New(ids.KindConfig), OwnershipPlanID: task.PlanID, OwnershipGeneration: 1,
		ImageRepository: "example/resolver", ImageIndexDigest: strings.Repeat("1", 64), ImageChildDigest: strings.Repeat("2", 64),
		ImageConfigDigest: strings.Repeat(
			"3",
			64,
		), ImageReference: "example/resolver@sha256:" + strings.Repeat("2", 64),
		ImageOS: "linux", ImageArchitecture: "amd64", ArtifactSHA256: task.PlanHash, ArtifactLength: 1,
		PlanSHA256: task.PlanHash, ExecutionPlanSHA256: task.PlanHash,
		EnsureService: true, ExpectedPreviousArtifactSHA256: task.PlanHash, ExpectedPreviousArtifactID: ids.New(ids.KindConfig),
		ExpectedPreviousGeneration: 1, PriorObservationModRevision: 1, PriorObservationRevision: 1, PredecessorTaskID: ids.New(ids.KindTask),
	}
	artifact := func(id, digest string) *agentpb.ComposeArtifact {
		decoded, err := hex.DecodeString(digest)
		if err != nil {
			t.Fatal(err)
		}
		return &agentpb.ComposeArtifact{ArtifactId: id, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
			ProjectName: "groundplane-infra", Services: []*agentpb.ComposeService{{ServiceId: input.GeneratedServiceID, ImageConfigDigest: decoded}}}
	}
	input.ComposeArtifact = artifact(input.ComposeArtifactID, input.ImageConfigDigest)
	input.RollbackComposeArtifact = artifact(input.ExpectedPreviousArtifactID, strings.Repeat("4", 64))
	raw, err := platformcomponents.EncodePlatformComponentTaskRenderInput(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), platformcomponents.PlatformComponentTaskRenderInputKey(task.PlanID), raw); err != nil {
		t.Fatal(err)
	}
	engine := &removalEngine{images: []imagefetch.LocalImage{
		{ID: "sha256:" + strings.Repeat("9", 64)}, // unrelated nginx-like content
		{ID: "sha256:" + input.ImageConfigDigest}, // untagged candidate config
		{ID: "sha256:" + strings.Repeat("4", 64)}, // untagged recovery config
	}}
	service := removalServiceFactory(t, store, tasks, engine)()
	for _, status := range []taskjournal.TaskStatus{taskjournal.TaskStatusRunning, taskjournal.TaskStatusFailed, taskjournal.TaskStatusTimedOut, taskjournal.TaskStatusAborted} {
		attempt, err := etcd.TransitionTaskStatus(
			task,
			taskjournal.TaskStatusPending,
			taskjournal.TaskStatusRunning,
			now.Add(time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		if status != taskjournal.TaskStatusRunning {
			attempt, err = etcd.TransitionTaskStatus(
				attempt,
				taskjournal.TaskStatusRunning,
				status,
				now.Add(2*time.Second),
			)
			if err != nil {
				t.Fatal(err)
			}
		}
		encoded, err := etcd.EncodeTaskRecord(attempt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Put(t.Context(), taskjournal.TaskStorageKey(task.ID), encoded); err != nil {
			t.Fatal(err)
		}
		inventory, err := service.ListImages(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if (inventory.Images[0].RemovalBlocked == "") != taskjournal.IsTerminalTaskStatus(status) ||
			inventory.Images[1].RemovalBlocked == "" || inventory.Images[2].RemovalBlocked == "" {
			t.Fatalf("%s: incorrect image protection: %#v", status, inventory.Images)
		}
	}
	if _, err := store.Delete(t.Context(), platformcomponents.PlatformComponentTaskRenderInputKey(task.PlanID)); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RemoveImage(t.Context(), engine.images[0].ID, "missing-retention-proof"); err == nil {
		t.Fatal("missing image authority allowed removal")
	}
}

// IMG-02: a failed Blueprint must retain its historical projection rather than
// locking unrelated images or consulting only the latest Environment head.
func TestImageRemovalRetainsFailedBlueprintImages(t *testing.T) {
	testBlueprintExecutedArtifact(
		t,
		false,
		false,
		func(fixture *ExecutedArtifactFixture, _ *taskplanning.TaskPlanResolver, render releaserender.ReleaseRenderInput, _ domain.Intent, _ *agentpb.ComposeArtifact) {
			page, err := fixture.store.Range(
				t.Context(),
				keyvalue.RangeRequest{Prefix: taskjournal.TaskPrefix, Limit: 128},
			)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, value := range page.Values {
				task, err := etcd.DecodeTaskRecord(value.Value)
				if err != nil {
					t.Fatal(err)
				}
				if task.PlanID != render.PlanID {
					// This shared fixture also publishes later pending work. Isolate
					// the failed attempt, retaining all of its runtime/Release inputs.
					if _, err := fixture.store.Delete(t.Context(), value.Key); err != nil {
						t.Fatal(err)
					}
					continue
				}
				found = true
				// The shared release fixture calls the lower-level preparer directly;
				// supply the procedure normally stamped by Blueprint admission.
				task.Params[taskcontract.EnvironmentBlueprintProcedureParam] = string(
					taskcontract.BlueprintComposeProcedureCandidateReleases,
				)
				task.Status = taskjournal.TaskStatusFailed
				task.Result = nil
				task.TerminalAssignment = nil
				raw, err := etcd.EncodeTaskRecord(task)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.store.Put(t.Context(), value.Key, raw); err != nil {
					t.Fatal(err)
				}
			}
			if !found {
				t.Fatal("fixture Blueprint Task not found")
			}
			engine := &removalEngine{
				images: []imagefetch.LocalImage{
					{ID: "sha256:" + strings.Repeat("f", 64)},
					{ID: render.CandidateWorkload.LocalImageID},
				},
			}
			service := removalServiceFactory(t, fixture.store.memoryHierarchyStore, fixture.Tasks, engine)()
			inventory, err := service.ListImages(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if inventory.Images[0].RemovalBlocked != "" || inventory.Images[1].RemovalBlocked == "" {
				t.Fatalf("failed Blueprint protection: %#v", inventory.Images)
			}
			if _, err := service.RemoveImage(t.Context(), engine.images[0].ID, "remove-unrelated-to-blueprint"); err != nil {
				t.Fatal(err)
			}
		},
	)
}
