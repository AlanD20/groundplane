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
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testattachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
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
		resolver *testtaskplanning.TaskPlanResolver, original testreleaserender.ReleaseRenderInput, _ domain.Intent,
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
	for _, race := range []string{
		"none", "before publication", "at commit", "source before publication", "source at commit",
		"before claim", "before acknowledgement",
	} {
		t.Run(race, func(t *testing.T) {
			testEntryMutationServingRuntime(t, race, false, core.ServiceRuntimeIntentRunning)
		})
	}
}

// Rationale: Entry edits must preserve a completed Attach that is newer than
// the serving Release. Capture and publication use the same Environment epoch.
func TestEntryMutationPreservesNewerAttachNetwork(t *testing.T) {
	for _, race := range []string{"none", "before publication", "at commit"} {
		t.Run(race, func(t *testing.T) {
			testEntryMutationServingRuntime(t, race, true, core.ServiceRuntimeIntentRunning)
		})
	}
}

// Rationale: a retained serving Release does not authorize startup after Stop or
// Destroy. Entry capture, publication and stored-plan reconstruction must agree.
func TestEntryMutationPreservesNonRunningIntent(t *testing.T) {
	for _, intent := range []core.ServiceRuntimeIntent{core.ServiceRuntimeIntentStopped, core.ServiceRuntimeIntentAbsent} {
		t.Run(string(intent), func(t *testing.T) { testEntryMutationServingRuntime(t, "none", true, intent) })
	}
}

func testEntryMutationServingRuntime(
	t *testing.T, race string, withAttach bool, runtimeIntent core.ServiceRuntimeIntent,
) {
	configure := func(project *composetypes.Project) {
		if withAttach {
			service := project.Services["api"]
			service.NetworkMode = ""
			project.Services["api"] = service
		}
	}
	testBlueprintExecutedArtifactConfigured(t, true, false, configure, func(fixture *etcd.ExecutedArtifactFixture,
		resolver *testtaskplanning.TaskPlanResolver, original testreleaserender.ReleaseRenderInput, originalIntent domain.Intent,
		_ *agentpb.ComposeArtifact) {
		ctx := t.Context()
		project, err := testcomposerender.LoadNormalizedEnvironmentProject(ctx, original.Projection)
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
		fixture.SeedEntryRuntimeIntent(t, original.ServiceID, runtimeIntent)
		authorityView, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, original.EnvironmentID)
		if err != nil || !found {
			t.Fatal("runtime authority view", err)
		}
		prior, err := fixture.Ledger.GetReleaseRenderInputAt(
			ctx,
			intent.PriorServingReleaseID,
			authorityView.ReadRevision,
		)
		if err != nil {
			t.Fatal(err)
		}
		authority := testreleaserender.ServiceLifecycleRelease{ServingReleaseID: currentRelease.ReleaseID,
			PriorServingReleaseID: intent.PriorServingReleaseID, Current: currentRelease, RetainedPrior: &prior.Record}
		fragments, err := resolver.RenderRetainedServiceRuntime(ctx, authority)
		if err != nil {
			t.Fatal(err)
		}
		if withAttach {
			for index, fragment := range fragments {
				fragments[index], err = testtaskplanning.MutateAttachNetworkArtifact(
					ctx,
					fragment,
					authorityView.Record,
					[]testattachrender.AttachTaskNetworkJoin{
						{NetworkID: networkID, ServiceIDs: []string{original.ServiceID}},
					},
					fragment.ArtifactId,
				)
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		fixture.SeedEntryAcknowledgedRuntime(t, currentRelease, fragments)
		current, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, original.EnvironmentID)
		if err != nil || !found {
			t.Fatal("desired projection", err)
		}
		captured, err := resolver.CaptureEntryMutationRuntime(ctx, current)
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(
			captured.RunningServiceIDs,
			original.ServiceID,
		) != (runtimeIntent == core.ServiceRuntimeIntentRunning) {
			t.Fatal("Entry capture lost operational intent")
		}
		entry, err := testentries.NewBlueprintRecord(original.EnvironmentID, "floor-probe",
			core.EnvEntry{ID: ids.New(ids.KindEnvEntry), Kind: core.EntryKindEnv,
				Key: "FLOOR_PROBE", Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"}},
			ids.New(ids.KindConfig))
		if err != nil {
			t.Fatal(err)
		}
		candidate, materials, err := testcomposerender.ProjectEnvironmentEntryMutation(
			captured.Projection, testcomposerender.EnvironmentEntryArtifactMutation{
				RevisionID:       ids.New(ids.KindTask),
				ArtifactID:       ids.New(ids.KindConfig),
				PlanID:           ids.New(ids.KindPlan),
				RenderGeneration: current.Record.RenderGeneration + 1,
				Entries:          []testentries.Record{entry},
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
		if !green && runtimeIntent == core.ServiceRuntimeIntentRunning {
			t.Fatal("Entry candidate omits serving api--green after rollback")
		}
		if withAttach && runtimeIntent == core.ServiceRuntimeIntentRunning {
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

func proveEntryServingPlanReconstruction(
	t *testing.T,
	fixture *etcd.ExecutedArtifactFixture,
	captured testtaskplanning.EntryMutationRuntime,
	desired, candidate testenvironmentprojection.EnvironmentComposeProjection,
	materials []testcomposerender.EnvironmentEntryMaterialization,
) etcd.TaskRecord {
	t.Helper()
	task := fixture.Task(t, 961)
	task.ID, task.RenderGeneration = candidate.RevisionID, int32(candidate.RenderGeneration)
	digest := sha256.Sum256([]byte("FLOOR_PROBE=one\n"))
	for _, material := range materials {
		task.Materializations = append(task.Materializations, testtaskmaterialization.Record{
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
		projections: map[string]testenvironmentprojection.EnvironmentComposeProjection{
			desired.RevisionID:   desired,
			candidate.RevisionID: candidate,
		}}
	resolver, err := testtaskplanning.NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
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
	if len(captured.RunningServiceIDs) == 0 {
		if len(plan.Steps) != len(task.Materializations) {
			t.Fatal("stored Entry plan would start a stopped or absent Service")
		}
		for _, step := range plan.Steps {
			if step.GetComposeApply() != nil {
				t.Fatal("materialization-only Entry plan contains a runtime apply")
			}
		}
		return task
	}
	names, err := executionplan.EntryMutationServices(plan, plan.Steps[len(plan.Steps)-1].StepId)
	if err != nil || !slices.Equal(names, []string{"api--blue", "api--green"}) {
		t.Fatalf("Entry execution selection=%v, %v; want both retained workloads without proxy", names, err)
	}
	return task
}

type entryServingPlanReader struct {
	*etcd.HierarchyRepository
	projections map[string]testenvironmentprojection.EnvironmentComposeProjection
}

func (reader *entryServingPlanReader) GetEnvironmentComposeProjectionRevision(
	ctx context.Context, environmentID, revisionID string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	projection, found := reader.projections[revisionID]
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: projection,
	}, found, ctx.Err()
}
