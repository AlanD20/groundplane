package taskplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters/custom"
	"github.com/AlanD20/groundplane/internal/adapters/postgres16"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testattachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

var (
	registerAttachPlanPostgres sync.Once
	registerAttachPlanCustom   sync.Once
)

// Rationale: a durable Attach task must rebuild its pinned external-network artifact and typed identity
// procedure without persisting the plaintext password or consulting mutable environment topology.
func TestTaskPlanResolverBuildsAttachNetworkAndAdapterProcedure(t *testing.T) {
	registerAttachPlanPostgres.Do(postgres16.Register)
	fixture := newAttachPlanFixture(t, "postgres:16", true)
	plan, err := fixture.resolver.ResolveExecutionPlan(context.Background(), fixture.task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_ATTACH || len(plan.Artifacts) != 1 ||
		len(plan.Steps) != 3 ||
		plan.Steps[0].GetAdapterProcedure().Phase !=
			agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION || string(plan.Steps[0].GetAdapterProcedure().Password) != "URL_safe-1" ||
		plan.Steps[1].GetAdapterProcedure().GrantOn != "other_4a1b2c" ||
		plan.Steps[2].GetComposeApply() == nil ||
		!slices.Equal(plan.Steps[2].GetComposeApply().ServiceIds, []string{fixture.record.ServiceID}) ||
		!bytes.Contains(plan.Artifacts[0].CanonicalYaml, []byte("gp_net_"+fixture.networkID)) ||
		!bytes.Contains(plan.Artifacts[0].CanonicalYaml, []byte("external: true")) {
		t.Fatalf("ResolveExecutionPlan() = %#v", plan)
	}
}

func TestTaskPlanResolverUsesDesiredAttachProjectionSnapshots(t *testing.T) {
	registerAttachPlanCustom.Do(custom.Register)
	fixture := newAttachPlanFixture(t, "custom", false)
	if _, err := fixture.resolver.ResolveExecutionPlan(context.Background(), fixture.task); err != nil {
		t.Fatalf("ResolveExecutionPlan() error with desired projection snapshots = %v", err)
	}
}

// Rationale: the locked custom adapter performs only the same pinned external-network reconciliation and
// must never resolve credentials or manufacture an empty adapter procedure.
func TestTaskPlanResolverBuildsCustomNetworkOnlyAttach(t *testing.T) {
	registerAttachPlanCustom.Do(custom.Register)
	fixture := newAttachPlanFixture(t, "custom", false)
	plan, err := fixture.resolver.ResolveExecutionPlan(context.Background(), fixture.task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	if len(plan.Artifacts) != 1 || len(plan.Steps) != 1 || plan.Steps[0].GetComposeApply() == nil ||
		fixture.state.identityCalls != 0 {
		t.Fatalf("custom ResolveExecutionPlan() = %#v, identity calls = %d", plan, fixture.state.identityCalls)
	}
}

// Rationale: live Attach mutates the captured serving native workload, keeps
// current Entry bindings, and never starts its stable proxy or inactive slot.
func TestTaskPlanResolverAttachesCapturedNativeWorkload(t *testing.T) {
	registerAttachPlanCustom.Do(custom.Register)
	fixture := newAttachPlanFixture(t, "custom", false)
	makeNativeAttachRuntime(t, &fixture)
	plan, err := fixture.resolver.ResolveExecutionPlan(t.Context(), fixture.task)
	if err != nil {
		t.Fatal(err)
	}
	artifact := plan.Artifacts[0]
	captured := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(fixture.state.renderInput.RuntimeProjection.ComposeArtifact, captured); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(artifact.CanonicalYaml, []byte("api--blue:")) ||
		!bytes.Contains(artifact.CanonicalYaml, []byte("env_file:")) ||
		!bytes.Contains(artifact.CanonicalYaml, []byte("gp_attach_")) || len(artifact.Services) != 2 ||
		artifact.Services[0].Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY ||
		artifact.Services[1].Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
		t.Fatalf("native Attach artifact lost captured runtime: %s", artifact.CanonicalYaml)
	}
	for index := range captured.Services {
		if !proto.Equal(captured.Services[index], artifact.Services[index]) {
			t.Fatalf("native Attach rewrote historical Service ownership: %#v", artifact.Services[index])
		}
	}
	request := &agentpb.ComposeHelperRequest{
		Schema: composehelper.SchemaVersion, Plan: plan, StepId: plan.Steps[0].StepId,
		TimeoutSeconds: 60, TaskId: fixture.task.ID,
		AssignmentId: ids.New(ids.KindAssignment), OperationId: ids.New(ids.KindOperation),
	}
	selected, err := composehelper.StartupServices(request)
	if err != nil || len(selected) != 1 || selected[0].ComposeName != "api--blue" ||
		selected[0].Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
		t.Fatalf("native Attach startup selection = %#v, %v", selected, err)
	}
	fake := runner.NewFake()
	if _, err := composehelper.Execute(t.Context(), fake, request); err != nil {
		t.Fatal(err)
	}
	calls := fake.RecordedCalls()
	args := calls[len(calls)-1].Args
	up := slices.Index(args, "up")
	if up < 0 || !slices.Equal(args[up:], []string{"up", "--detach", "--no-deps", "api--blue"}) {
		t.Fatalf("native Attach helper widened selection: %v", args)
	}
}

func makeNativeAttachRuntime(t *testing.T, fixture *attachPlanFixture) {
	t.Helper()
	_, _, release := redeployRestorationInput(t, domain.StrategyBlueGreen)
	runtime := release.Members[0].Render.Projection
	runtime.ComposeArtifact = append([]byte(nil), release.Members[0].Render.PriorRuntime.CurrentArtifact...)
	if runtime.EnvironmentID != fixture.reader.projection.EnvironmentID ||
		runtime.DesiredServices[0].Desired.ID != fixture.record.ServiceID {
		t.Fatal("native Attach fixture identities diverged")
	}
	entry := testentries.Record{EnvironmentID: runtime.EnvironmentID,
		Entry: core.EnvEntry{ID: ids.New(ids.KindEnvEntry), Kind: core.EntryKindEnv,
			Key: "MODE", Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"}},
		CurrentValueGenerationID: ids.New(ids.KindConfig)}
	runtime.Entries = []testentries.Record{entry}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(runtime.ComposeArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	artifact, err := testcomposerender.MutateEnvironmentEntryArtifact(
		artifact,
		runtime,
		testcomposerender.EnvironmentEntryArtifactMutation{
			ArtifactID: artifact.ArtifactId, Entries: runtime.Entries,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	fixture.reader.projection = runtime
	fixture.state.renderInput.RuntimeProjection = runtime
	fixture.state.renderInput.Services = attachPlanServiceSnapshots(runtime.DesiredServices)
	fixture.state.renderInput.Networks = attachPlanOwnedNetworkSnapshots(runtime.DesiredZones)
	fixture.state.renderInput.Volumes = append(
		[]testenvironmentprojection.EnvironmentVolumeIdentity(nil),
		runtime.Volumes...)
	fixture.state.renderInput.VolumeMounts = append(
		[]testenvironmentprojection.EnvironmentServiceVolumeMount(nil),
		runtime.VolumeMounts...)
	fixture.state.renderInput.RunningServiceIDs = []string{fixture.record.ServiceID}
}

func TestTaskPlanResolverReconcilesInactiveAttachWithoutActivatingProfile(t *testing.T) {
	registerAttachPlanCustom.Do(custom.Register)
	fixture := newAttachPlanFixture(t, "custom", false)
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(fixture.reader.projection.ComposeArtifact, artifact); err != nil {
		t.Fatalf("unmarshal normalized Compose artifact: %v", err)
	}
	artifact.CanonicalYaml = bytes.Replace(
		artifact.CanonicalYaml,
		[]byte("    image: example/api:latest"),
		[]byte("    image: example/api:latest\n    profiles: [configured]"),
		1,
	)
	digest := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = digest[:]
	artifact.Services[0].ExpectedReplicas = 0
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatalf("marshal inactive normalized Compose artifact: %v", err)
	}
	fixture.reader.projection.ComposeArtifact = encoded
	fixture.reader.projection.NormalizedCompose = bytes.Replace(
		fixture.reader.projection.NormalizedCompose,
		[]byte("    image: example/api:latest"),
		[]byte("    image: example/api:latest\n    profiles: [configured]"),
		1,
	)
	fixture.state.renderInput.RuntimeProjection = fixture.reader.projection
	fixture.state.renderInput.RunningServiceIDs = nil
	plan, err := fixture.resolver.ResolveExecutionPlan(context.Background(), fixture.task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	if len(plan.Steps) != 1 || plan.Steps[0].GetComposeApply() == nil ||
		len(plan.Steps[0].GetComposeApply().ServiceIds) != 0 || len(plan.Artifacts[0].Services) != 1 ||
		plan.Artifacts[0].Services[0].ExpectedReplicas != 0 ||
		!bytes.Contains(plan.Artifacts[0].CanonicalYaml, []byte("external: true")) {
		t.Fatalf("inactive Attach plan = %#v", plan)
	}
	request := &agentpb.ComposeHelperRequest{
		Schema: composehelper.SchemaVersion, Plan: plan, StepId: plan.Steps[0].StepId,
		TimeoutSeconds: 60, TaskId: fixture.task.ID,
		AssignmentId: ids.New(ids.KindAssignment), OperationId: ids.New(ids.KindOperation),
	}
	fake := runner.NewFake()
	if _, err := composehelper.Execute(t.Context(), fake, request); err != nil {
		t.Fatal(err)
	}
	calls := fake.RecordedCalls()
	if len(calls) != 1 || slices.Contains(calls[0].Args, "up") {
		t.Fatalf("configured-only Attach started runtime: %v", calls)
	}
}

// Rationale: detach must revoke grants, reconcile the pinned artifact without the removed membership, and
// only then deprovision the backing identity.
func TestTaskPlanResolverOrdersDetachNetworkRemoval(t *testing.T) {
	registerAttachPlanPostgres.Do(postgres16.Register)
	fixture := newAttachPlanFixture(t, "postgres:16", true)
	makeNativeAttachRuntime(t, &fixture)
	fixture.record.Status = core.AttachDetaching
	fixture.record.Operation = testattachments.AttachOperationDetach
	fixture.state.attaches[fixture.record.ID] = testkeyvalue.Versioned[testattachments.Record]{Record: fixture.record}
	fixture.state.renderInput.NetworkJoins = nil
	fixture.task.Type = testtaskjournal.TaskDetach
	plan, err := fixture.resolver.ResolveExecutionPlan(context.Background(), fixture.task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(detach) error = %v", err)
	}
	if len(plan.Steps) != 3 ||
		plan.Steps[0].GetAdapterProcedure().Phase != agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE ||
		plan.Steps[1].GetComposeApply() == nil ||
		plan.Steps[2].GetAdapterProcedure().Phase != agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH ||
		bytes.Contains(plan.Artifacts[0].CanonicalYaml, []byte("gp_net_"+fixture.networkID)) {
		t.Fatalf("ResolveExecutionPlan(detach) = %#v", plan)
	}
	request := &agentpb.ComposeHelperRequest{
		Schema: composehelper.SchemaVersion, Plan: plan, StepId: plan.Steps[1].StepId,
		TimeoutSeconds: 60, TaskId: fixture.task.ID,
		AssignmentId: ids.New(ids.KindAssignment), OperationId: ids.New(ids.KindOperation),
	}
	selected, err := composehelper.StartupServices(request)
	if err != nil || len(selected) != 1 || selected[0].ComposeName != "api--blue" {
		t.Fatalf("native Detach startup selection = %#v, %v", selected, err)
	}
}

type attachPlanFixture struct {
	resolver  *TaskPlanResolver
	state     *attachPlanTestState
	reader    *blueprintPlanReader
	task      etcd.TaskRecord
	record    testattachments.Record
	networkID string
}

func newAttachPlanFixture(t *testing.T, adapterKey string, withGrant bool) attachPlanFixture {
	t.Helper()
	now := time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC)
	reader, _ := blueprintPlanTestState(t)
	attachID := ids.NewAt(ids.KindAttach, now, 1)
	grantID := ids.NewAt(ids.KindAttach, now, 2)
	taskID := ids.NewAt(ids.KindTask, now, 3)
	backingServiceID := ids.NewAt(ids.KindService, now, 4)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, now, 5)
	networkID := ids.NewAt(ids.KindNetwork, now, 20)
	consumerServiceID := reader.projection.DesiredServices[0].Desired.ID
	consumerServiceName := reader.projection.DesiredServices[0].Desired.Name
	ownedNetworkID := reader.projection.DesiredZones[0].Desired.ID
	ownedNetworkName := reader.projection.DesiredZones[0].Desired.Name
	reader.projection.DesiredServices = []testservices.EnvironmentServiceProjection{{
		EnvironmentID: reader.environment.ID,
		Desired:       core.Service{ID: consumerServiceID, Name: consumerServiceName},
	}}
	reader.projection.DesiredZones = []testenvironmentprojection.EnvironmentZoneProjection{{
		EnvironmentID: reader.environment.ID,
		Desired:       core.Zone{ID: ownedNetworkID, Name: ownedNetworkName},
	}}
	grantIDs := []string(nil)
	factSets := []testattachments.FactSetMetadata(nil)
	if withGrant {
		grantIDs = []string{grantID}
		factSets = []testattachments.FactSetMetadata{
			{Facts: []testattachments.FactDefinition{{Key: "pg16_DATABASE"}}},
			{GrantAttachID: grantID, Facts: []testattachments.FactDefinition{{Key: "pg16_DATABASE"}}},
		}
	}
	record, err := testattachments.NewPendingAttachRecord(
		attachID, reader.environment.ID, "api-db", ids.NewAt(ids.KindProject, now, 7),
		backingEnvironmentID, backingServiceID, networkID, consumerServiceID, attachID, grantIDs, factSets, taskID, now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	attaches := map[string]testkeyvalue.Versioned[testattachments.Record]{attachID: {Record: record}}
	identity := AttachPlanIdentity{Database: "api_5d3f9a", Role: "api_5d3f9a", Password: []byte("URL_safe-1")}
	if withGrant {
		grant := record
		grant.ID = grantID
		grant.GrantAttachIDs = nil
		grant.Status = core.AttachReady
		grant.TaskID = ids.NewAt(ids.KindTask, now, 9)
		attaches[grantID] = testkeyvalue.Versioned[testattachments.Record]{Record: grant}
		identity.Grants = []AttachPlanGrantIdentity{{AttachID: grantID, Database: "other_4a1b2c"}}
	}
	service := testservices.ServiceRecord{
		EnvironmentID: backingEnvironmentID, BackingNetworkID: networkID,
		Desired: core.Service{ID: backingServiceID, Name: "postgres", Adapter: adapterKey},
		Runtime: core.ServiceRuntime{
			ServiceID: backingServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
		},
	}
	input := testattachrender.AttachTaskRenderInput{
		PlanID:                   ids.NewAt(ids.KindPlan, now, 10),
		AttachID:                 attachID,
		AttachName:               record.Name,
		TenantID:                 reader.tenant.ID,
		TenantSlug:               reader.tenant.Slug,
		ProjectID:                reader.project.ID,
		ProjectSlug:              reader.project.Slug,
		EnvironmentID:            reader.environment.ID,
		EnvironmentName:          reader.environment.Name,
		AuthorizedVolumeDir:      reader.environment.VolumeDir,
		BackingServiceID:         backingServiceID,
		BackingProjectID:         record.BackingProjectID,
		AdapterKey:               adapterKey,
		DesiredRevisionID:        reader.revision.RevisionID,
		ArtifactID:               ids.NewAt(ids.KindConfig, now, 21),
		RenderGeneration:         reader.projection.RenderGeneration,
		EnvironmentEpochRevision: 1,
		RuntimeProjection:        reader.projection,
		RunningServiceIDs:        []string{consumerServiceID},
		Services: []testattachrender.AttachTaskServiceSnapshot{{
			ID: consumerServiceID, Name: consumerServiceName,
		}},
		Networks: []testattachrender.AttachTaskOwnedNetworkSnapshot{{
			ID: ownedNetworkID, Name: ownedNetworkName,
		}},
		Volumes: append([]testenvironmentprojection.EnvironmentVolumeIdentity(nil), reader.projection.Volumes...),
		NetworkJoins: []testattachrender.AttachTaskNetworkJoin{
			{NetworkID: networkID, ServiceIDs: []string{consumerServiceID}},
		},
		ConsumerServiceIDs: []string{record.ServiceID},
		GrantAttachIDs:     append([]string(nil), record.GrantAttachIDs...),
	}
	state := &attachPlanTestState{
		attaches: attaches, service: testkeyvalue.Versioned[testservices.ServiceRecord]{Record: service},
		identity: identity, renderInput: input,
	}
	resolver, err := NewTaskPlanResolverWithAttachments(
		"/var/lib/groundplane/vol", reader, state, state, state,
		nil,
	)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithAttachments() error = %v", err)
	}
	stepCount := len(identity.Grants) + 2
	if adapterKey == "custom" {
		stepCount = 1
	}
	steps := make([]testtaskjournal.TaskStepRecord, stepCount)
	for index := range steps {
		steps[index] = testtaskjournal.TaskStepRecord{
			Kind: testtaskjournal.TaskStepOperation,
			ID:   ids.NewAt(ids.KindStep, now, 11+int64(index)),
		}
	}
	task := etcd.TaskRecord{
		ID: taskID, Executor: testtaskjournal.TaskExecutorAgent, PlanID: input.PlanID,
		RenderGeneration: int32(input.RenderGeneration), Type: testtaskjournal.TaskAttach, Target: attachID,
		Params:         map[string]string{testtaskjournal.TaskMutationEnvironmentParam: record.EnvironmentID},
		TimeoutSeconds: 60, Steps: steps,
	}
	return attachPlanFixture{
		resolver: resolver, state: state, reader: reader, task: task, record: record, networkID: networkID,
	}
}

type attachPlanTestState struct {
	attaches        map[string]testkeyvalue.Versioned[testattachments.Record]
	service         testkeyvalue.Versioned[testservices.ServiceRecord]
	identity        AttachPlanIdentity
	renderInput     testattachrender.AttachTaskRenderInput
	identityCalls   int
	blueprintIntent *testattachments.BlueprintAttachTaskIntent
}

func (state *attachPlanTestState) GetBlueprintAttachTaskIntent(
	context.Context, string,

) (testkeyvalue.Versioned[testattachments.BlueprintAttachTaskIntent], bool, error) {
	if state.blueprintIntent != nil {
		return testkeyvalue.Versioned[testattachments.BlueprintAttachTaskIntent]{
			Record:   *state.blueprintIntent,
			Revision: 1,
		}, true, nil
	}
	return testkeyvalue.Versioned[testattachments.BlueprintAttachTaskIntent]{}, false, nil
}

func (state *attachPlanTestState) GetAttach(
	_ context.Context,
	id string,
) (testkeyvalue.Versioned[testattachments.Record], error) {
	return state.attaches[id], nil
}

func (state *attachPlanTestState) GetAttachTaskRenderInput(
	context.Context, string,

) (testkeyvalue.Versioned[testattachrender.AttachTaskRenderInput], error) {
	return testkeyvalue.Versioned[testattachrender.AttachTaskRenderInput]{Record: state.renderInput}, nil
}

func (state *attachPlanTestState) GetService(
	context.Context, string,

) (testkeyvalue.Versioned[testservices.ServiceRecord], error) {
	return state.service, nil
}

func (state *attachPlanTestState) ResolveTaskIdentity(
	_ context.Context,
	_ testkeyvalue.Versioned[testattachments.Record],
	_ string,
	consume AttachPlanIdentityConsumer,
) error {
	state.identityCalls++
	identity := state.identity
	identity.Password = append([]byte(nil), state.identity.Password...)
	identity.Grants = append([]AttachPlanGrantIdentity(nil), state.identity.Grants...)
	defer identity.Clear()
	return consume(identity)
}
