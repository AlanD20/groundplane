package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterializationowner "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

type publicationSizeImages struct{}

func (publicationSizeImages) ResolveWorkloadImages(
	_ context.Context, _ string, selectors []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	resolved := make([]*agentpb.WorkloadImageResolution, len(selectors))
	for index, selector := range selectors {
		resolved[index] = &agentpb.WorkloadImageResolution{
			Selector: proto.CloneOf(selector), LocalImageId: "sha256:" + strings.Repeat("a", 64),
		}
	}
	return &agentpb.WorkloadImageResolutionResult{RequestId: strings.Repeat("1", 32),
		Outcome: &agentpb.WorkloadImageResolutionResult_Success{Success: &agentpb.WorkloadImageResolutions{
			Resolutions: resolved,
		}}}, nil
}

// Rationale: two running Services repeat the same legal Environment projection
// in the aggregate marker. Their individual render and assignment bounds fit;
// the update must publish and retain exact recovery/terminal bytes without
// enlarging the durable record ceiling or dropping historical witnesses.
func TestBlueprintRunningUpdateBoundedPublication(t *testing.T) {
	ctx := context.Background()
	fixture := NewExecutedArtifactFixture(t)
	audit := fixture.AuditBlueprintPublicationSize()
	resolver, err := testtaskplanning.NewTaskPlanResolverWithBlueprints(
		"/var/lib/groundplane/vol",
		fixture.Hierarchy,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := resolver.EnableReleasePlans(fixture.Ledger); err != nil {
		t.Fatal(err)
	}
	if err := resolver.EnableConfigurationRecovery(fixture.ConfigurationSources(t)); err != nil {
		t.Fatal(err)
	}
	scripts, sources := fixture.HookDependencies(t)
	if err := resolver.EnableScriptPlans(scripts); err != nil {
		t.Fatal(err)
	}
	artifacts, err := testtaskplanning.NewScriptArtifactService(scripts, unexpectedHookEntryResolver{t: t})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := blueprintrelease.NewService(fixture.Ledger, scripts, resolver, artifacts, sources,
		blueprintImageLookupAgent(t, fixture), publicationSizeImages{})
	if err != nil {
		t.Fatal(err)
	}
	tenant, err := fixture.Hierarchy.GetTenant(ctx, fixture.Project.Record.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	serviceIDs := []string{ids.New(ids.KindService), ids.New(ids.KindService)}
	previous := &composetypes.Project{Name: "test", Services: composetypes.Services{}}
	var priorArtifact []byte
	for pass := 0; pass < 2; pass++ {
		task := fixture.Task(t, 2600+int64(pass))
		task.CreatedAt = time.Now().UTC()
		task.UpdatedAt = task.CreatedAt
		task.RenderGeneration = int32(pass + 1)
		artifactID := ids.New(ids.KindConfig)
		task.Params[taskcontract.EnvironmentBlueprintArtifactParam] = artifactID
		task.Params[taskcontract.EnvironmentBlueprintProcedureParam] = string(
			taskcontract.BlueprintComposeProcedureNone,
		)
		project := &composetypes.Project{Name: "test", Services: composetypes.Services{}}
		changes := make([]testblueprints.EnvironmentBlueprintServiceChange, len(serviceIDs))
		identities := make([]testcomposeidentity.Resource, len(serviceIDs))
		projection := testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID:    fixture.Environment.Record.ID,
			RevisionID:       task.ID,
			RenderGeneration: uint64(pass + 1),
		}
		for index, serviceID := range serviceIDs {
			name := fmt.Sprintf("worker-%d", index)
			desired := core.Service{ID: serviceID, Name: name, Image: "example/api:1",
				Replicas: pass + 1, Strategy: core.StrategyRecreate}
			record, err := testservices.NewServiceRecord(fixture.Environment.Record.ID, desired, "")
			if err != nil {
				t.Fatal(err)
			}
			changes[index].Record = record
			if pass > 0 {
				scope, err := fixture.Ledger.LoadPlanningScope(ctx, fixture.Environment.Record.ID)
				if err != nil {
					t.Fatal(err)
				}
				planning, err := fixture.Ledger.LoadPlanningServices(ctx, scope, []string{serviceID})
				if err != nil || len(planning) != 1 || planning[0].Projection.ServingReleaseID == "" {
					t.Fatalf("running predecessor: %v", err)
				}
				changes[index].Current = &planning[0].Service
			}
			definition := composetypes.ServiceConfig{Name: name, Image: desired.Image, Scale: &desired.Replicas,
				NetworkMode: "none", Environment: composetypes.MappingWithEquals{},
				HealthCheck: &composetypes.HealthCheckConfig{Test: []string{"CMD", "true"}}}
			for field := 0; field < 8; field++ {
				value := strings.Repeat("x", 3000)
				definition.Environment[fmt.Sprintf("PUBLIC_FIXTURE_%d", field)] = &value
			}
			project.Services[name] = definition
			identities[index] = testcomposeidentity.Resource{ID: serviceID, Name: name}
			projection.DesiredServices = append(
				projection.DesiredServices,
				testservices.EnvironmentServiceProjection{
					EnvironmentID: fixture.Environment.Record.ID,
					Desired:       desired,
				},
			)
		}
		memberships, err := blueprintrelease.BuildNormalizedServiceMemberships(previous, project)
		if err != nil {
			t.Fatal(err)
		}
		workloads, err := producer.Preflight(ctx, fixture.Environment.Record.ID, changes, memberships, nil)
		if err != nil {
			t.Fatalf("pass%d preflight: %v", pass, err)
		}
		artifact, err := testcomposerender.RenderCompose(
			testcomposerender.ComposeRenderInput{Project: project, ArtifactID: artifactID,
				ProjectOwnerKind: testcomposerender.ComposeProjectOwnerTenant, TenantID: tenant.Record.ID,
				ProjectID: fixture.Project.Record.ID, EnvironmentID: fixture.Environment.Record.ID, PlanID: task.PlanID,
				RenderGeneration: uint64(pass + 1), AuthorizedVolumeDir: fixture.Environment.Record.VolumeDir,
				Identities: testcomposeidentity.Snapshot{Services: identities}},
		)
		if err != nil {
			t.Fatal(err)
		}
		projection.ComposeArtifact, err = proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
		if err != nil {
			t.Fatal(err)
		}
		projection.NormalizedCompose, err = project.MarshalYAML()
		if err != nil {
			t.Fatal(err)
		}
		prefix := prepareBlueprintFileFixture(t, &task, &projection, artifactID, pass)
		environment, err := fixture.Hierarchy.GetEnvironment(ctx, fixture.Environment.Record.ID)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := producer.Prepare(ctx, blueprintrelease.PrepareInput{Workloads: workloads,
			VolumeRoot: "/var/lib/groundplane/vol", Task: task, Projection: projection, Tenant: tenant,
			Project: fixture.Project, Environment: environment, Memberships: memberships, PrefixSteps: prefix,
			ServiceChanges: changes, Artifact: artifact, CreatedAt: task.CreatedAt,
			AllocateNamed: func(kind ids.Kind, purpose string) string {
				return ids.DeriveAt(kind, task.CreatedAt, task.ID, purpose)
			}})
		if err != nil {
			t.Fatalf("pass%d real producer publication: %v", pass, err)
		}
		if pass > 0 {
			fixture.AssertBlueprintNativePublicationSourceFences(t, prepared.Task, prepared.Publication)
		}
		fixture.Publish(t, prepared.Task, projection, prepared.Publication)
		if pass > 0 {
			fixture.AssertBlueprintNativeReferenceRejection(t, prepared.Task)
		}
		frozen, err := fixture.Ledger.GetBlueprintTaskRenderInput(ctx, prepared.Task)
		if err != nil || len(frozen.Members) != 2 {
			t.Fatalf("pass%d immutable publication read: %v", pass, err)
		}
		replayed, err := resolver.ResolveExecutionPlan(ctx, prepared.Task)
		if err != nil || !proto.Equal(prepared.Plan, replayed) {
			t.Fatalf("pass%d immutable plan replay: %v", pass, err)
		}
		assertBlueprintFileRecoveryFixture(t, prepared.Task, replayed, pass)
		agentID := ids.New(ids.KindAgent)
		claim, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
		if err != nil || !found || claim.Task.Record.ID != task.ID {
			t.Fatalf("pass%d assignment: %t %v", pass, found, err)
		}
		if pass > 0 {
			authority := claim.Assignment.Record.RestorationAuthority
			if authority == nil || authority.AppliedPredecessor == nil ||
				!bytes.Equal(
					authority.AppliedPredecessor.ComposeArtifact,
					priorArtifact,
				) || len(authority.NativePredecessors) != 2 {
				t.Fatal("assignment lost the exact applied or native predecessor witnesses")
			}
			for index, native := range frozen.NativePredecessors {
				if native.Serving == nil ||
					!bytes.Equal(native.CurrentArtifact, authority.NativePredecessors[index].CurrentArtifact) {
					t.Fatal("assignment changed the native predecessor artifact")
				}
			}
		}
		fixture.AssertBlueprintPublicationRecordSizes(t, prepared.Task, claim)
		publicationComparisons, assignmentComparisons := 27, 14
		if pass > 0 {
			publicationComparisons, assignmentComparisons = 40, 18
		}
		if audit.Publication.Comparisons != publicationComparisons || audit.Publication.Mutations != 19 ||
			audit.Assignment.Comparisons != assignmentComparisons || audit.Assignment.Mutations != 7 {
			t.Fatalf("pass%d changed transaction shapes: publication=%+v assignment=%+v", pass,
				audit.Publication, audit.Assignment)
		}
		if audit.Publication.Comparisons == 0 || audit.Publication.Mutations == 0 || audit.Publication.Bytes > 1<<20 ||
			audit.Assignment.Comparisons+audit.Assignment.Mutations > 96 || audit.Assignment.Bytes > 1<<20 {
			t.Fatal("complete publication/assignment transaction exceeds its existing budget")
		}
		proveBoundedBlueprintAgentAdmission(t, claim, replayed)
		result := testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose, ExecutionEpoch: 1,
			Diagnostic: testtaskjournal.TaskResultDiagnosticNone}
		completed, err := fixture.Tasks.AcknowledgeTask(
			ctx,
			agentID,
			1,
			task.ID,
			claim.Assignment.Record.AssignmentID,
			testtaskjournal.TaskStatusCompleted,
			result,
			task.CreatedAt.Add(time.Minute),
		)
		if err != nil || completed.Record.Status != testtaskjournal.TaskStatusCompleted {
			t.Fatalf("pass%d terminal completion: %v", pass, err)
		}
		replayedTerminal, err := fixture.Tasks.AcknowledgeTask(
			ctx,
			agentID,
			1,
			task.ID,
			claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, result,
			task.CreatedAt.Add(2*time.Minute),
		)
		if err != nil || replayedTerminal.Revision != completed.Revision {
			t.Fatalf("pass%d terminal replay: %v", pass, err)
		}
		latest, present, err := fixture.Hierarchy.GetEnvironmentAppliedComposeProjection(ctx, task.Target)
		executed, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(prepared.Plan.Artifacts[0])
		if err != nil || marshalErr != nil || !present || !bytes.Equal(latest.Record.ComposeArtifact, executed) {
			t.Fatal("terminal completion did not retain the exact executed artifact")
		}
		previous, priorArtifact = project, executed
	}
}

func prepareBlueprintFileFixture(
	t *testing.T,
	task *etcd.TaskRecord,
	projection *testenvironmentprojection.EnvironmentComposeProjection,
	artifactID string,
	pass int,
) []*agentpb.ExecutionStep {
	t.Helper()
	content := []byte("configuration " + strconv.Itoa(pass))
	digest := sha256.Sum256(content)
	const destination = "config/runtime.yaml"
	projection.RuntimeFiles = []core.BlueprintFile{{Path: destination, Content: content}}
	record := testtaskmaterializationowner.Record{
		StepID:            ids.New(ids.KindStep),
		MaterializationID: ids.New(ids.KindConfig),
		EnvironmentID:     task.Target,
		Destination:       destination,
		OutputKind:        testtaskmaterializationowner.OutputPlainFile,
		UID: uint32(
			100 + pass,
		),
		GID:    100,
		Mode:   0o444,
		Length: uint64(len(content)),
		SHA256: hex.EncodeToString(digest[:]),
		Source: testtaskmaterializationowner.Source{Kind: testtaskmaterializationowner.SourceBlueprintFile,
			BlueprintFile: &testtaskmaterializationowner.BlueprintFileValueReference{
				RevisionID: task.ID,
				Path:       destination,
			}},
	}
	task.Materializations = []testtaskmaterializationowner.Record{record}
	step, err := testtaskmaterialization.BuildTaskMaterializationStep(record, artifactID, uint32(task.TimeoutSeconds))
	if err != nil {
		t.Fatal(err)
	}
	return []*agentpb.ExecutionStep{step}
}

// SVC-15/JOURNEY-02: publication, replay and claim retain the previous pass's
// bytes and owner, independently of this pass's desired output.
func assertBlueprintFileRecoveryFixture(t *testing.T, task etcd.TaskRecord, plan *agentpb.ExecutionPlan, pass int) {
	t.Helper()
	configuration := plan.GetCandidateReleaseProcedure().GetConfigurationRestoration()
	if len(configuration.GetFiles()) != 1 || task.Configuration == nil ||
		configuration.Files[0].ForwardStepId != task.Materializations[0].StepID {
		t.Fatal("published candidate lost its closed file recovery binding")
	}
	var probe *agentpb.MaterializeFile
	for _, step := range plan.Steps {
		if step.StepId == configuration.Files[0].ProbeStepId {
			probe = step.GetMaterializeFile()
		}
	}
	if probe == nil {
		t.Fatal("file recovery probe absent after reconstruction")
	}
	if pass == 0 {
		if configuration.PriorSnapshotId != "" ||
			probe.OutputKind != agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_PLAIN_FILE {
			t.Fatal("first pass invented pre-existing configuration")
		}
		return
	}
	digest := sha256.Sum256([]byte("configuration 0"))
	if task.Configuration.Prior == nil || configuration.PriorSnapshotId != task.Configuration.Prior.ID ||
		probe.Uid != 100 || probe.Gid != 100 || probe.Mode != 0o444 || probe.Length != 15 ||
		hex.EncodeToString(probe.Sha256) != hex.EncodeToString(digest[:]) {
		t.Fatal("second pass did not pin exact previously acknowledged file bytes and owner")
	}
}
