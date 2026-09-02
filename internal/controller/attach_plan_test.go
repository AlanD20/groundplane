package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters/manual"
	"github.com/AlanD20/groundplane/internal/adapters/postgres16"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

var (
	registerAttachPlanPostgres sync.Once
	registerAttachPlanManual   sync.Once
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
			agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION ||
		string(plan.Steps[0].GetAdapterProcedure().Password) != "URL_safe-1" ||
		plan.Steps[1].GetAdapterProcedure().GrantOn != "other_4a1b2c" ||
		plan.Steps[2].GetComposeApply() == nil ||
		!slices.Equal(plan.Steps[2].GetComposeApply().ServiceIds, []string{fixture.record.ServiceID}) ||
		!bytes.Contains(plan.Artifacts[0].CanonicalYaml, []byte("gp_net_"+fixture.networkID)) ||
		!bytes.Contains(plan.Artifacts[0].CanonicalYaml, []byte("external: true")) {
		t.Fatalf("ResolveExecutionPlan() = %#v", plan)
	}
}

func TestTaskPlanResolverUsesDesiredAttachProjectionSnapshots(t *testing.T) {
	registerAttachPlanManual.Do(manual.Register)
	fixture := newAttachPlanFixture(t, "manual", false)
	if _, err := fixture.resolver.ResolveExecutionPlan(context.Background(), fixture.task); err != nil {
		t.Fatalf("ResolveExecutionPlan() error with desired projection snapshots = %v", err)
	}
}

// Rationale: the locked manual adapter performs only the same pinned external-network reconciliation and
// must never resolve credentials or manufacture an empty adapter procedure.
func TestTaskPlanResolverBuildsManualNetworkOnlyAttach(t *testing.T) {
	registerAttachPlanManual.Do(manual.Register)
	fixture := newAttachPlanFixture(t, "manual", false)
	plan, err := fixture.resolver.ResolveExecutionPlan(context.Background(), fixture.task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	if len(plan.Artifacts) != 1 || len(plan.Steps) != 1 || plan.Steps[0].GetComposeApply() == nil ||
		fixture.state.identityCalls != 0 {
		t.Fatalf("manual ResolveExecutionPlan() = %#v, identity calls = %d", plan, fixture.state.identityCalls)
	}
}

func TestTaskPlanResolverReconcilesInactiveAttachWithoutActivatingProfile(t *testing.T) {
	registerAttachPlanManual.Do(manual.Register)
	fixture := newAttachPlanFixture(t, "manual", false)
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
	plan, err := fixture.resolver.ResolveExecutionPlan(context.Background(), fixture.task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	apply := plan.Steps[0].GetComposeApply()
	if apply == nil || !apply.FullReconcile || len(apply.ServiceIds) != 0 || len(plan.Artifacts[0].Services) != 1 ||
		plan.Artifacts[0].Services[0].ExpectedReplicas != 0 ||
		!bytes.Contains(plan.Artifacts[0].CanonicalYaml, []byte("external: true")) {
		t.Fatalf("inactive Attach plan = %#v", plan)
	}
}

// Rationale: detach must revoke grants, reconcile the pinned artifact without the removed membership, and
// only then deprovision the backing identity.
func TestTaskPlanResolverOrdersDetachNetworkRemoval(t *testing.T) {
	registerAttachPlanPostgres.Do(postgres16.Register)
	fixture := newAttachPlanFixture(t, "postgres:16", true)
	fixture.record.Status = core.AttachDetaching
	fixture.record.Operation = etcd.AttachOperationDetach
	fixture.state.attaches[fixture.record.ID] = etcd.Versioned[etcd.AttachRecord]{Record: fixture.record}
	fixture.state.renderInput.NetworkJoins = nil
	fixture.task.Type = etcd.TaskDetach
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
}

type attachPlanFixture struct {
	resolver  *TaskPlanResolver
	state     *attachPlanTestState
	reader    *blueprintPlanReader
	task      etcd.TaskRecord
	record    etcd.AttachRecord
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
	reader.projection.DesiredServices = []etcd.EnvironmentServiceProjection{{
		EnvironmentID: reader.environment.ID,
		Desired:       core.Service{ID: consumerServiceID, Name: consumerServiceName},
	}}
	reader.projection.DesiredZones = []etcd.EnvironmentZoneProjection{{
		EnvironmentID: reader.environment.ID,
		Desired:       core.Zone{ID: ownedNetworkID, Name: ownedNetworkName},
	}}
	grantIDs := []string(nil)
	factSets := []etcd.AttachFactSetMetadata(nil)
	if withGrant {
		grantIDs = []string{grantID}
		factSets = []etcd.AttachFactSetMetadata{
			{Facts: []etcd.AttachFactDefinition{{Key: "pg16_DATABASE"}}},
			{GrantAttachID: grantID, Facts: []etcd.AttachFactDefinition{{Key: "pg16_DATABASE"}}},
		}
	}
	record, err := etcd.NewPendingAttachRecord(
		attachID, reader.environment.ID, "api-db", ids.NewAt(ids.KindProject, now, 7),
		backingEnvironmentID, backingServiceID, networkID, consumerServiceID, attachID, grantIDs, factSets, taskID, now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	attaches := map[string]etcd.Versioned[etcd.AttachRecord]{attachID: {Record: record}}
	identity := AttachPlanIdentity{Database: "api_5d3f9a", Role: "api_5d3f9a", Password: []byte("URL_safe-1")}
	if withGrant {
		grant := record
		grant.ID = grantID
		grant.GrantAttachIDs = nil
		grant.Status = core.AttachReady
		grant.TaskID = ids.NewAt(ids.KindTask, now, 9)
		attaches[grantID] = etcd.Versioned[etcd.AttachRecord]{Record: grant}
		identity.Grants = []AttachPlanGrantIdentity{{AttachID: grantID, Database: "other_4a1b2c"}}
	}
	service := etcd.ServiceRecord{
		EnvironmentID: backingEnvironmentID, BackingNetworkID: networkID,
		Desired: core.Service{ID: backingServiceID, Name: "postgres", Adapter: adapterKey},
		Runtime: core.ServiceRuntime{
			ServiceID: backingServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
		},
	}
	input := etcd.AttachTaskRenderInput{
		PlanID:              ids.NewAt(ids.KindPlan, now, 10),
		AttachID:            attachID,
		AttachName:          record.Name,
		TenantID:            reader.tenant.ID,
		TenantSlug:          reader.tenant.Slug,
		ProjectID:           reader.project.ID,
		ProjectSlug:         reader.project.Slug,
		EnvironmentID:       reader.environment.ID,
		EnvironmentName:     reader.environment.Name,
		AuthorizedVolumeDir: reader.environment.VolumeDir,
		BackingServiceID:    backingServiceID,
		BackingProjectID:    record.BackingProjectID,
		AdapterKey:          adapterKey,
		DesiredRevisionID:   reader.revision.RevisionID,
		ArtifactID:          ids.NewAt(ids.KindConfig, now, 21),
		RenderGeneration:    reader.projection.RenderGeneration,
		Services: []etcd.AttachTaskServiceSnapshot{{
			ID: consumerServiceID, Name: consumerServiceName,
		}},
		Networks: []etcd.AttachTaskOwnedNetworkSnapshot{{
			ID: ownedNetworkID, Name: ownedNetworkName,
		}},
		Volumes: append([]etcd.EnvironmentVolumeIdentity(nil), reader.projection.Volumes...),
		NetworkJoins: []etcd.AttachTaskNetworkJoin{
			{NetworkID: networkID, ServiceIDs: []string{consumerServiceID}},
		},
		ConsumerServiceIDs: []string{record.ServiceID},
		GrantAttachIDs:     append([]string(nil), record.GrantAttachIDs...),
	}
	state := &attachPlanTestState{
		attaches: attaches, service: etcd.Versioned[etcd.ServiceRecord]{Record: service},
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
	if adapterKey == "manual" {
		stepCount = 1
	}
	steps := make([]etcd.TaskStepRecord, stepCount)
	for index := range steps {
		steps[index] = etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: ids.NewAt(ids.KindStep, now, 11+int64(index))}
	}
	task := etcd.TaskRecord{
		ID: taskID, Executor: etcd.TaskExecutorAgent, PlanID: input.PlanID,
		RenderGeneration: int32(input.RenderGeneration), Type: etcd.TaskAttach, Target: attachID,
		Params:         map[string]string{etcd.TaskMutationEnvironmentParam: record.EnvironmentID},
		TimeoutSeconds: 60, Steps: steps,
	}
	return attachPlanFixture{
		resolver: resolver, state: state, reader: reader, task: task, record: record, networkID: networkID,
	}
}

type attachPlanTestState struct {
	attaches        map[string]etcd.Versioned[etcd.AttachRecord]
	service         etcd.Versioned[etcd.ServiceRecord]
	identity        AttachPlanIdentity
	renderInput     etcd.AttachTaskRenderInput
	identityCalls   int
	blueprintIntent *etcd.BlueprintAttachTaskIntent
}

func (state *attachPlanTestState) GetBlueprintAttachTaskIntent(
	context.Context,
	string,
) (etcd.Versioned[etcd.BlueprintAttachTaskIntent], bool, error) {
	if state.blueprintIntent != nil {
		return etcd.Versioned[etcd.BlueprintAttachTaskIntent]{Record: *state.blueprintIntent, Revision: 1}, true, nil
	}
	return etcd.Versioned[etcd.BlueprintAttachTaskIntent]{}, false, nil
}

func (state *attachPlanTestState) GetAttach(
	_ context.Context,
	id string,
) (etcd.Versioned[etcd.AttachRecord], error) {
	return state.attaches[id], nil
}

func (state *attachPlanTestState) GetAttachTaskRenderInput(
	context.Context,
	string,
) (etcd.Versioned[etcd.AttachTaskRenderInput], error) {
	return etcd.Versioned[etcd.AttachTaskRenderInput]{Record: state.renderInput}, nil
}

func (state *attachPlanTestState) GetService(
	context.Context,
	string,
) (etcd.Versioned[etcd.ServiceRecord], error) {
	return state.service, nil
}

func (state *attachPlanTestState) ResolveTaskIdentity(
	_ context.Context,
	_ etcd.Versioned[etcd.AttachRecord],
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
