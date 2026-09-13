package etcd_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"testing"

	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// Rationale: Attach reads the immutable Blueprint root, while Entry reads its
// published head. Those keys can have different etcd modification revisions
// even when they identify exactly the same desired runtime at the capture read.
func TestEntryMutationCapturesImmutableRevisionSource(t *testing.T) {
	testBlueprintExecutedArtifact(t, true, false, func(fixture *etcd.ExecutedArtifactFixture,
		resolver *controller.TaskPlanResolver, original etcd.ReleaseRenderInput, _ domain.Intent,
		_ *agentpb.ComposeArtifact) {
		ctx := t.Context()
		if err := resolver.EnableReleasePlans(fixture.Ledger); err != nil {
			t.Fatal(err)
		}
		current, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, original.EnvironmentID)
		if err != nil || !found {
			t.Fatal("desired head", err)
		}
		root, found, err := fixture.Hierarchy.GetEnvironmentComposeProjectionRevision(ctx,
			original.EnvironmentID, current.Record.RevisionID)
		if err != nil || !found {
			t.Fatal("immutable root", err)
		}
		if root.Revision == current.Revision {
			t.Fatal("fixture must stage the immutable root before publishing the head")
		}
		captured, err := resolver.CaptureEntryMutationRuntime(ctx, root)
		if err != nil {
			t.Fatal("identical desired runtime from immutable root rejected", err)
		}
		etcd.AssertAttachRuntimeRoundTrip(t, captured.Projection, captured.EpochRevision, captured.RunningServiceIDs)
		for _, mismatch := range []string{"revision identity", "artifact bytes"} {
			t.Run(mismatch, func(t *testing.T) {
				changed := root
				if mismatch == "revision identity" {
					changed.Record.RevisionID = ids.New(ids.KindTask)
				} else {
					changed.Record.ComposeArtifact = []byte("different immutable artifact")
				}
				if _, err := resolver.CaptureEntryMutationRuntime(t.Context(), changed); !errors.Is(
					err,
					errs.New(errs.KindStateConflict, ""),
				) {
					t.Fatalf("changed desired runtime was not rejected: %v", err)
				}
			})
		}
	})
}

// Rationale: a successful native rollback changes the serving slot without
// advancing desired Blueprint state. Entry changes must reach that slot, not
// merely complete a Task against the older desired runtime.
func TestEntryMutationCapturesServingRuntimeAfterRollback(t *testing.T) {
	for _, race := range []string{"none", "before publication", "at commit"} {
		t.Run(race, func(t *testing.T) { testEntryMutationServingRuntime(t, race, false) })
	}
}

// Rationale: Entry edits must preserve a completed Attach that is newer than
// the serving Release. Capture and publication use the same Environment epoch.
func TestEntryMutationPreservesNewerAttachNetwork(t *testing.T) {
	for _, race := range []string{"none", "before publication", "at commit"} {
		t.Run(race, func(t *testing.T) { testEntryMutationServingRuntime(t, race, true) })
	}
}

func testEntryMutationServingRuntime(t *testing.T, race string, withAttach bool) {
	configure := func(project *composetypes.Project) {
		if withAttach {
			service := project.Services["api"]
			service.NetworkMode = ""
			project.Services["api"] = service
		}
	}
	testBlueprintExecutedArtifactConfigured(t, true, false, configure, func(fixture *etcd.ExecutedArtifactFixture,
		resolver *controller.TaskPlanResolver, original etcd.ReleaseRenderInput, originalIntent domain.Intent,
		_ *agentpb.ComposeArtifact) {
		ctx := t.Context()
		project, err := controller.LoadNormalizedEnvironmentProject(ctx, original.Projection)
		if err != nil {
			t.Fatal(err)
		}
		proveRetainedBlueprintProducer(t, fixture, resolver, project, original.ServiceID, false)
		currentRelease, intent := fixture.SeedRetainedRollback(t, original, originalIntent)
		currentRelease = fixture.SeedNativeBlueGreenPredecessor(t, currentRelease, intent)
		if err := resolver.EnableReleasePlans(fixture.Ledger); err != nil {
			t.Fatal(err)
		}
		var networkID string
		if withAttach {
			networkID = fixture.SeedEntryRuntimeAttach(t, original.ServiceID)
		}
		current, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, original.EnvironmentID)
		if err != nil || !found {
			t.Fatal("desired projection", err)
		}
		captured, err := resolver.CaptureEntryMutationRuntime(ctx, current)
		if err != nil {
			t.Fatal(err)
		}
		entry, err := etcd.NewBlueprintEntryRecord(original.EnvironmentID, "floor-probe",
			core.EnvEntry{ID: ids.New(ids.KindEnvEntry), Kind: core.EntryKindEnv,
				Key: "FLOOR_PROBE", Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"}},
			ids.New(ids.KindConfig))
		if err != nil {
			t.Fatal(err)
		}
		candidate, materials, err := controller.ProjectEnvironmentEntryMutation(
			captured.Projection,
			controller.EnvironmentEntryArtifactMutation{
				RevisionID:       ids.New(ids.KindTask),
				ArtifactID:       ids.New(ids.KindConfig),
				PlanID:           ids.New(ids.KindPlan),
				RenderGeneration: current.Record.RenderGeneration + 1,
				Entries:          []etcd.EntryRecord{entry},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		artifact := &agentpb.ComposeArtifact{}
		if err := proto.Unmarshal(candidate.ComposeArtifact, artifact); err != nil {
			t.Fatal(err)
		}
		var green bool
		for _, service := range artifact.Services {
			if service.ComposeName == "api--green" {
				green = true
				for _, label := range service.ExpectedLabels {
					if label.Key == "com.groundplane.release-id" && label.Value != currentRelease.ReleaseID {
						t.Fatal("Entry changed serving Release ownership")
					}
				}
			}
		}
		if !green {
			t.Fatal("Entry candidate omits serving api--green after rollback")
		}
		if withAttach {
			var document struct {
				Services map[string]struct {
					Networks map[string]any `yaml:"networks"`
				} `yaml:"services"`
			}
			if err := yaml.Unmarshal(artifact.CanonicalYaml, &document); err != nil {
				t.Fatal(err)
			}
			if _, found := document.Services["api--green"].Networks["gp_attach_"+strings.ToLower(networkID)]; !found {
				t.Fatal("Entry edit discarded the newer native Attach network")
			}
		}
		task := proveEntryServingPlanReconstruction(t, fixture, captured, current.Record, candidate, materials)
		proveEntryServingPublication(t, fixture, current, candidate, task, race)
	})
}

func proveEntryServingPlanReconstruction(t *testing.T, fixture *etcd.ExecutedArtifactFixture,
	captured controller.EntryMutationRuntime, desired, candidate etcd.EnvironmentComposeProjection,
	materials []controller.EnvironmentEntryMaterialization,
) etcd.TaskRecord {
	t.Helper()
	task := fixture.Task(t, 961)
	task.ID, task.RenderGeneration = candidate.RevisionID, int32(candidate.RenderGeneration)
	digest := sha256.Sum256([]byte("FLOOR_PROBE=one\n"))
	for _, material := range materials {
		task.Materializations = append(task.Materializations, etcd.TaskMaterializationRecord{
			StepID: ids.New(ids.KindStep), MaterializationID: ids.New(ids.KindConfig),
			EnvironmentID: candidate.EnvironmentID, Destination: material.Destination,
			ServiceID: material.ServiceID, ServiceName: material.ServiceName, OutputKind: material.OutputKind,
			UID: material.UID, GID: material.GID, Mode: uint32(material.Mode),
			Length: 16, SHA256: hex.EncodeToString(digest[:]), Source: material.Source,
		})
	}
	task, err := captured.PrepareTask("/var/lib/groundplane/vol", task, candidate,
		ids.New(ids.KindStep))
	if err != nil {
		t.Fatal("prepare captured Entry plan", err)
	}
	reader := &entryServingPlanReader{HierarchyRepository: fixture.Hierarchy,
		projections: map[string]etcd.EnvironmentComposeProjection{
			desired.RevisionID:   desired,
			candidate.RevisionID: candidate,
		}}
	resolver, err := controller.NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.ResolveExecutionPlan(t.Context(), task)
	if err != nil {
		t.Fatal("reconstruct captured Entry plan from immutable desired revisions", err)
	}
	if hex.EncodeToString(plan.PlanHash) != task.PlanHash {
		t.Fatal("reconstructed Entry plan differs from published hash")
	}
	names, err := executionplan.EntryMutationServices(plan, plan.Steps[len(plan.Steps)-1].StepId)
	if err != nil || !slices.Equal(names, []string{"api--blue", "api--green"}) {
		t.Fatalf("Entry execution selection=%v, %v; want both retained workloads without proxy", names, err)
	}
	return task
}

type entryServingPlanReader struct {
	*etcd.HierarchyRepository
	projections map[string]etcd.EnvironmentComposeProjection
}

func (reader *entryServingPlanReader) GetEnvironmentComposeProjectionRevision(
	ctx context.Context, environmentID, revisionID string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	projection, found := reader.projections[revisionID]
	return etcd.Versioned[etcd.EnvironmentComposeProjection]{Record: projection}, found, ctx.Err()
}
