package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testlocalagents "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ExecutedArtifactFixture exposes only the existing in-memory publisher fixture
// to an external-package test that can use the actual Controller renderer.
type ExecutedArtifactFixture struct {
	Hierarchy           *etcd.HierarchyRepository
	Ledger              *etcd.ReleaseLedger
	Tasks               *etcd.TaskRepository
	Project             testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	Environment         testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	store               *releasePlanningTestStore
	head                int64
	hookScripts         etcd.BlueprintScriptPublication
	TerminalCommitFault string
	publicationSize     *BlueprintPublicationSizeAudit
}

func (fixture *ExecutedArtifactFixture) ReadRevision() int64 { return fixture.store.revision }

// SeedNativeCandidateDesired models a completed native deploy's retained
// desired replica count without changing the applied Blueprint witness.
func (fixture *ExecutedArtifactFixture) SeedNativeCandidateDesired(
	t *testing.T,
	render testreleaserender.ReleaseRenderInput,
	replicas int,
) {
	t.Helper()
	projection := testenvironmentprojection.CloneEnvironmentComposeProjection(render.Projection)
	projection.RevisionID = ids.New(ids.KindTask)
	projection.RenderGeneration = 2
	for index := range projection.DesiredServices {
		if projection.DesiredServices[index].Desired.ID == render.ServiceID {
			projection.DesiredServices[index].Desired.Replicas = replicas
		}
	}
	task := environmentBlueprintTestTask(
		t,
		fixture.Project.Record,
		fixture.Environment.Record,
		int64(projection.RenderGeneration)+3000,
	)
	task.ID = projection.RevisionID
	marker := environmentBlueprintTestMarker(task, render.EnvironmentID)
	stageEnvironmentBlueprintForPublicationTest(
		t,
		fixture.store,
		fixture.head,
		environmentBlueprintTestRevision(render.EnvironmentID, task, string(projection.NormalizedCompose)),
		projection,
		marker,
	)
	headValue, err := testidempotency.EncodeTaskReference(projection.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(headValue)
	if _, err := fixture.store.Put(
		t.Context(),
		testblueprints.EnvironmentBlueprintHeadKey(render.EnvironmentID),
		headValue,
	); err != nil {
		t.Fatal(err)
	}
	loaded, err := fixture.store.Get(
		context.Background(),
		testblueprints.EnvironmentBlueprintHeadKey(render.EnvironmentID),
	)
	if err != nil || loaded.Entry == nil {
		t.Fatalf("desired fixture head: %v", err)
	}
	fixture.head = loaded.Entry.ModRevision
}

func (fixture *ExecutedArtifactFixture) AssertNativeCandidateSourceRace(
	t *testing.T,
	task etcd.TaskRecord,
	projection testenvironmentprojection.EnvironmentComposeProjection,
	publication etcd.BlueprintReleasePublication,
	source testreleaserender.ReleaseRenderInput,
	fault string,
) {
	t.Helper()
	ctx := context.Background()
	key := map[string]string{"intent": testreleases.ReleaseIntentStagingKey("", source.ReleaseID), "render": testreleases.ReleaseRenderInputStagingKey("", source.ReleaseID),
		"projection": testreleases.ReleaseProjectionKey(source.ServiceID), "applied": testenvironmentprojection.EnvironmentComposeProjectionStorageKey(source.EnvironmentID)}[strings.Split(fault, "-")[0]]
	if strings.HasPrefix(fault, "inactive-") {
		serving, err := fixture.Ledger.ResolveServing(ctx, source.EnvironmentID, source.ServiceID, 0)
		if err != nil || serving.Intent.PriorServingReleaseID == "" {
			t.Fatalf("inactive race source: %v", err)
		}
		key = testreleases.ReleaseRenderInputStagingKey("", serving.Intent.PriorServingReleaseID)
	}
	before, err := fixture.store.Get(ctx, key)
	if err != nil || before.Entry == nil {
		t.Fatalf("source race fixture: %v", err)
	}
	head, err := fixture.store.Get(ctx, testblueprints.EnvironmentBlueprintHeadKey(task.Target))
	if err != nil || head.Entry == nil {
		t.Fatalf("desired head before race: %v", err)
	}
	mutation := testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: before.Entry.Value}
	if strings.HasSuffix(fault, "-prune") {
		mutation.Type, mutation.Value = testkeyvalue.MutationDelete, nil
	}
	if _, err := fixture.store.Transact(ctx, nil, []testkeyvalue.Mutation{mutation}); err != nil {
		t.Fatal(err)
	}
	result, err := fixture.tryPublish(t, task, projection, publication)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || !errors.Is(conflict, errs.New(errs.KindStateConflict, "")) ||
		outcome == etcd.IdempotencyKnownApplied {
		t.Fatalf("native source race published: %v %v %v", outcome, conflict, err)
	}
	loaded, err := fixture.store.GetMany(
		ctx, testkeyvalue.GetManyRequest{
			Keys: []string{
				testtaskjournal.TaskStorageKey(task.ID),
				testreleases.ReleasePublicationKey(task.Params[testreleaserender.TaskReleasePublicationParam]),
				testblueprints.EnvironmentBlueprintHeadKey(task.Target),
			},
		},
	)
	if err != nil || len(loaded.Values) != 3 || loaded.Values[0] != nil || loaded.Values[1] != nil ||
		loaded.Values[2] == nil ||
		loaded.Values[2].ModRevision != head.Entry.ModRevision ||
		!bytes.Equal(loaded.Values[2].Value, head.Entry.Value) {
		t.Fatalf("source race leaked Task/marker/desired head: %v", err)
	}
}

func (fixture *ExecutedArtifactFixture) SeedNativeBlueGreenPredecessor(
	t *testing.T,
	current testreleaserender.ReleaseRenderInput,
	intent domain.Intent,
) testreleaserender.ReleaseRenderInput {
	t.Helper()
	scope, err := fixture.Ledger.LoadPlanningScope(t.Context(), current.EnvironmentID)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := fixture.Ledger.GetReleaseRenderInputAt(t.Context(), intent.PriorServingReleaseID, scope.ReadRevision)
	if err != nil {
		t.Fatal(err)
	}
	current, prior.Record = testreleaserender.CloneReleaseRenderInput(
		current,
	), testreleaserender.CloneReleaseRenderInput(
		prior.Record,
	)
	prior.Record.Strategy, prior.Record.Slot, prior.Record.CandidateTarget = domain.StrategyBlueGreen, domain.SlotBlue, domain.WorkloadBlue
	current.Strategy, current.Slot, current.CandidateTarget = domain.StrategyBlueGreen, domain.SlotGreen, domain.WorkloadGreen
	current.PriorStrategy, current.PriorSlot, current.PriorTarget = domain.StrategyBlueGreen, domain.SlotBlue, domain.WorkloadBlue
	current.PriorArtifactID, current.PriorWorkload = "", nil
	current.PriorProxyGeneration, current.PriorProxyDigest = 0, ""
	for _, render := range []*testreleaserender.ReleaseRenderInput{&prior.Record, &current} {
		render.CandidateWorkload.ReplicaCount = 1
		render.Projection.DesiredServices[0].Desired.Replicas = 1
		for _, old := range []string{"replicas: 2", "replicas: 3", "scale: 2", "scale: 3"} {
			render.Projection.NormalizedCompose = bytes.ReplaceAll(
				render.Projection.NormalizedCompose,
				[]byte(old),
				[]byte(strings.Split(old, ":")[0]+": 1"),
			)
		}
		config, err := domain.RenderProxyConfig(
			render.ServiceName,
			render.ReleaseID,
			render.CandidateTarget,
			render.ProxyGeneration,
			render.ProxyPorts,
		)
		if err != nil {
			t.Fatal(err)
		}
		render.ProxyConfigDigest = hex.EncodeToString(config.SHA256[:])
		raw, err := testreleaserender.EncodeReleaseRenderInput(*render)
		if err != nil {
			t.Fatal(err)
		}
		value, err := testreleases.EncodeReleaseRecord("release-render-input", json.RawMessage(raw))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.Put(t.Context(), testreleases.ReleaseRenderInputStagingKey("", render.ReleaseID), value); err != nil {
			t.Fatal(err)
		}
		if render.ReleaseID == current.ReleaseID {
			intent.Strategy, intent.Slot, intent.CandidateWorkload = current.Strategy, current.Slot, current.CandidateWorkload
			intent.RenderInputDigest, err = domain.Digest(json.RawMessage(raw))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	value, err := testreleases.EncodeReleaseRecord("release-intent", intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Put(t.Context(), testreleases.ReleaseIntentStagingKey("", intent.ID), value); err != nil {
		t.Fatal(err)
	}
	return current
}

// ProveNativeCandidateRecovery supplies bounded observed proof to the real ACK
// state machine. It proves persistence/reconnect, not physical Docker recovery.
func (fixture *ExecutedArtifactFixture) ProveNativeCandidateRecovery(
	t *testing.T,
	agentID string,
	claim etcd.TaskAssignment,
	plan *agentpb.ExecutionPlan,
	priorReleaseID string,
) {
	t.Helper()
	ctx := context.Background()
	task, assignment := claim.Task.Record, claim.Assignment.Record
	authority := assignment.RestorationAuthority
	if authority == nil || len(authority.Candidates) != 1 || len(authority.NativePredecessors) != 1 {
		t.Fatal("native recovery lacks one sealed member")
	}
	member := plan.GetCandidateReleaseProcedure().GetMembers()[0]
	keys := []string{
		testenvironmentprojection.EnvironmentComposeProjectionStorageKey(task.Target),
		testreleases.ReleaseProjectionKey(member.ServiceId),
	}
	before, err := fixture.store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	for index, state := range []testtaskjournal.TaskEventState{testtaskjournal.TaskEventStateRunning, testtaskjournal.TaskEventStateCompleted} {
		_, err := fixture.Tasks.AppendTaskEvent(
			ctx,
			testtaskjournal.TaskEventInput{
				Identity: testtaskjournal.TaskEventIdentity{AssignmentID: assignment.AssignmentID, AgentID: agentID,
					AgentGeneration: 1, TaskID: task.ID, StepID: member.GetForwardStepIds()[0], Attempt: 1, Ordinal: uint64(index + 1)},
				State: state,
				Payload: json.RawMessage(
					`{"message":"native forward"}`,
				),
			},
			task.CreatedAt.Add(time.Duration(1100+index*100)*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	primary := testtaskjournal.TaskResultRecord{
		Kind:                   testtaskjournal.TaskResultCompose,
		Diagnostic:             testtaskjournal.TaskResultDiagnosticComposeFailed,
		ExitCode:               17,
		FailedStepID:           member.GetForwardStepIds()[1],
		ReconciliationRequired: true,
		ExecutionEpoch:         1,
	}
	transitioned, err := fixture.Tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		assignment.AssignmentID, testtaskjournal.TaskStatusFailed, primary,
		task.CreatedAt.Add(3*time.Second),
	)
	if err != nil || transitioned.Record.Status != testtaskjournal.TaskStatusRunning {
		t.Fatalf("native recovery transition: %v", err)
	}
	reopened, err := etcd.NewTaskRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	reconnected, err := reopened.ListAgentAssignments(ctx, agentID, 1, 1)
	if err != nil || len(reconnected) != 1 {
		t.Fatalf("native reconnect: %v", err)
	}
	recovery := reconnected[0]
	if recovery.Assignment.Record.AssignmentID != assignment.AssignmentID ||
		recovery.Assignment.Record.ExecutionEpoch != 2 ||
		recovery.ReleaseRecovery == nil ||
		recovery.Assignment.Record.RestorationAuthoritySHA256 != assignment.RestorationAuthoritySHA256 ||
		!bytes.Equal(
			recovery.Assignment.Record.RestorationAuthority.NativePredecessors[0].CurrentArtifact,
			authority.NativePredecessors[0].CurrentArtifact,
		) {
		t.Fatal("reconnect replaced immutable native C")
	}
	witness, err := testtaskassignments.OpenRestorationWitness(
		task.Target,
		authority.NativePredecessors[0].CurrentArtifact,
	)
	if err != nil {
		t.Fatal(err)
	}
	final := mixedRecoveryResult(
		t,
		recovery,
		plan.GetCandidateReleaseProcedure(),
		witness,
		member.ServiceId,
		priorReleaseID,
	)
	final.CandidateAbsenceEvidence = nil
	ordinal := uint64(1)
	for _, stepID := range recovery.ReleaseRecovery.StepIDs {
		for _, state := range []testtaskjournal.TaskEventState{testtaskjournal.TaskEventStateRunning, testtaskjournal.TaskEventStateCompleted} {
			_, err := reopened.AppendTaskEvent(
				ctx, testtaskjournal.TaskEventInput{
					Identity: testtaskjournal.TaskEventIdentity{
						AssignmentID:    assignment.AssignmentID,
						AgentID:         agentID,
						AgentGeneration: 1,
						TaskID:          task.ID,
						StepID:          stepID,
						Attempt:         2,
						Ordinal:         ordinal,
					},
					State:   state,
					Payload: json.RawMessage(`{"message":"native recovery"}`),
				}, task.CreatedAt.Add(time.Duration(4+ordinal)*time.Second),
			)
			if err != nil {
				t.Fatal(err)
			}
			ordinal++
		}
	}
	for _, mutate := range []func(*testtaskjournal.TaskResultRecord){
		func(result *testtaskjournal.TaskResultRecord) {
			result.ProxyEvidence, result.RecreateEvidence = nil, nil
		},
		func(result *testtaskjournal.TaskResultRecord) {
			if len(result.ProxyEvidence) != 0 {
				result.ProxyEvidence[0].ReleaseID = member.CandidateReleaseId
			} else {
				result.RecreateEvidence[0].ReleaseID = member.CandidateReleaseId
			}
		},
		func(result *testtaskjournal.TaskResultRecord) {
			if len(result.ProxyEvidence) != 0 {
				result.ProxyEvidence[0].Compensated = false
			} else {
				result.RecreateEvidence[0].Compensated = false
			}
		},
	} {
		changed := testtaskjournal.CloneTaskResult(&final)
		mutate(changed)
		revision := fixture.ReadRevision()
		if _, err := reopened.AcknowledgeTask(ctx, agentID, 1, task.ID, assignment.AssignmentID, testtaskjournal.TaskStatusCompleted, *changed, task.CreatedAt.Add(30*time.Second)); !errors.Is(
			err,
			errs.New(errs.KindStateConflict, ""),
		) &&
			!errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("wrong native recovery proof accepted: %v", err)
		}
		if fixture.ReadRevision() != revision {
			t.Fatal("rejected native proof changed durable state")
		}
	}
	terminal, err := reopened.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		assignment.AssignmentID, testtaskjournal.TaskStatusCompleted, final,
		task.CreatedAt.Add(31*time.Second),
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusFailed || terminal.Record.Result == nil ||
		terminal.Record.Result.ExitCode != 17 ||
		terminal.Record.Result.ReconciliationRequired {
		t.Fatalf("native recovery lost original failure: %v", err)
	}
	after, err := fixture.store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	for index, value := range before.Values {
		if value == nil || after.Values[index] == nil || value.ModRevision != after.Values[index].ModRevision ||
			!bytes.Equal(value.Value, after.Values[index].Value) {
			t.Fatal("recovery changed A or serving C")
		}
	}
	replay, err := reopened.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		assignment.AssignmentID, testtaskjournal.TaskStatusCompleted, final,
		task.CreatedAt.Add(32*time.Second),
	)
	if err != nil || replay.Revision != terminal.Revision || replay.Record.Status != testtaskjournal.TaskStatusFailed {
		t.Fatalf("native terminal replay changed authority: %v", err)
	}
}

func (fixture *ExecutedArtifactFixture) ImageLookupStore() testkeyvalue.Store {
	return newMemoryHierarchyStore()
}

func (fixture *ExecutedArtifactFixture) ImageLookupAgentRecord() testlocalagents.LocalAgentRecord {
	now := time.Date(2026, 8, 22, 19, 30, 0, 0, time.UTC)
	var token [agentprotocol.RawTokenBytes]byte
	for index := range token {
		token[index] = 7
	}
	digest := sha256.Sum256(token[:])
	return testlocalagents.LocalAgentRecord{
		ID:               ids.NewAt(ids.KindAgent, now, 41),
		EnrollmentTaskID: ids.NewAt(ids.KindTask, now, 40),
		Image:            "ghcr.io/groundplane/agent@sha256:" + strings.Repeat("a", 64),
		Generation:       1,
		Phase:            testlocalagents.LocalAgentPhaseProvisioning,
		Config: testlocalagents.LocalAgentConfig{
			PullIntervalSeconds: 5,
			MaxConcurrentTasks:  4,
			Labels:              map[string]string{"role": "local"},
		},
		EncryptedToken: []byte("age-encrypted-token"),
		TokenDigest:    base64.RawURLEncoding.EncodeToString(digest[:]),
		CreatedAt:      now,
		TokenUpdatedAt: now,
	}
}

// Rationale: a real candidate plus retained-source fragment must keep both
// candidate Task authority and every retained comparison, plus exact bytes.
func (fixture *ExecutedArtifactFixture) AssertMixedRuntimeTamperingRejected(
	t *testing.T,
	task etcd.TaskRecord,
	projection testenvironmentprojection.EnvironmentComposeProjection,
	publication etcd.BlueprintReleasePublication,
) {
	t.Helper()
	withoutCandidate := task
	withoutCandidate.Params = nil
	if _, err := fixture.tryPublish(t, withoutCandidate, projection, publication); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatal("mixed publication accepted missing candidate Task authority")
	}
	changedProjection := projection
	changedProjection.ComposeArtifact = append([]byte(nil), projection.ComposeArtifact...)
	changedProjection.ComposeArtifact = append(changedProjection.ComposeArtifact, 0xA0, 0x06, 0x01)
	if err := proto.Unmarshal(changedProjection.ComposeArtifact, &agentpb.ComposeArtifact{}); err != nil {
		t.Fatalf("tamper fixture must remain valid protobuf: %v", err)
	}
	if _, err := fixture.tryPublish(t, task, changedProjection, publication); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) && !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatal("mixed publication accepted changed staged artifact bytes")
	}
}

// Mutate only the independently owned serving pointer after capture. The
// applied Blueprint key stays byte-for-byte and revision-for-revision intact.
func (fixture *ExecutedArtifactFixture) RejectChangedServingPublication(
	t *testing.T,
	task etcd.TaskRecord,
	projection testenvironmentprojection.EnvironmentComposeProjection,
	publication etcd.BlueprintReleasePublication,
	serviceID string,
) {
	t.Helper()
	ctx := context.Background()
	before, err := fixture.store.Get(ctx, testenvironmentprojection.EnvironmentComposeProjectionStorageKey(task.Target))
	if err != nil || before.Entry == nil {
		t.Fatalf("applied before: %v", err)
	}
	record, err := fixture.store.Get(ctx, testreleases.ReleaseProjectionKey(serviceID))
	if err != nil || record.Entry == nil {
		t.Fatalf("serving before: %v", err)
	}
	current, err := testreleases.DecodeReleaseRecord[domain.ServiceProjection](
		record.Entry.Value,
		"service-release-projection",
	)
	if err != nil || current.ServingReleaseID == "" {
		t.Fatalf("serving decode: %v", err)
	}
	current.ServingReleaseID = ids.New(ids.KindDeployment)
	value, err := testreleases.EncodeReleaseRecord("service-release-projection", current)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Put(ctx, record.Entry.Key, value); err != nil {
		t.Fatal(err)
	}
	result, err := fixture.tryPublish(t, task, projection, publication)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || !errors.Is(conflict, errs.New(errs.KindStateConflict, "")) ||
		outcome == etcd.IdempotencyKnownApplied {
		t.Fatalf("stale serving accepted: %v %v %v", outcome, conflict, err)
	}
	after, err := fixture.store.Get(ctx, testenvironmentprojection.EnvironmentComposeProjectionStorageKey(task.Target))
	if err != nil || after.Entry == nil || after.Entry.ModRevision != before.Entry.ModRevision ||
		string(after.Entry.Value) != string(before.Entry.Value) {
		t.Fatal("serving-race fixture changed applied Blueprint authority")
	}
}

func NewExecutedArtifactFixture(t *testing.T) *ExecutedArtifactFixture {
	t.Helper()
	store := &releasePlanningTestStore{memoryHierarchyStore: newMemoryHierarchyStore()}
	hierarchy, err := etcd.NewHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, hierarchy)
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := etcd.NewReleaseLedger(store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	return &ExecutedArtifactFixture{
		Hierarchy:   hierarchy,
		Ledger:      ledger,
		Tasks:       tasks,
		Project:     project,
		Environment: environment,
		store:       store,
	}
}

func (fixture *ExecutedArtifactFixture) Task(t *testing.T, seed int64) etcd.TaskRecord {
	return environmentBlueprintTestTask(t, fixture.Project.Record, fixture.Environment.Record, seed)
}

func (fixture *ExecutedArtifactFixture) Publish(
	t *testing.T,
	task etcd.TaskRecord,
	projection testenvironmentprojection.EnvironmentComposeProjection,
	release etcd.BlueprintReleasePublication,
) {
	t.Helper()
	result, err := fixture.tryPublish(t, task, projection, release)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil ||
		outcome != etcd.IdempotencyKnownApplied {
		t.Fatalf("publication outcome=%v conflict=%v err=%v", outcome, conflict, err)
	}
	fixture.head = result.Revision()
}

func (fixture *ExecutedArtifactFixture) tryPublish(
	t *testing.T,
	task etcd.TaskRecord,
	projection testenvironmentprojection.EnvironmentComposeProjection,
	release etcd.BlueprintReleasePublication,
) (etcd.IdempotencyTransactionResult, error) {
	t.Helper()
	ctx := context.Background()
	marker := environmentBlueprintTestMarker(task, fixture.Environment.Record.ID)
	revision := environmentBlueprintTestRevision(
		fixture.Environment.Record.ID,
		task,
		string(projection.NormalizedCompose),
	)
	claim := stageEnvironmentBlueprintForPublicationTest(
		t,
		fixture.store,
		fixture.head,
		revision,
		projection,
		marker,
	)
	groups, err := fixture.Hierarchy.PrepareReleaseGroupBlueprintMutation(
		ctx,
		fixture.Environment.Record.ID,
		fixture.store.revision,
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	var scripts etcd.BlueprintScriptPublication
	if !fixture.hookScripts.IsZero() {
		scripts = fixture.hookScripts
		defer fixture.hookScripts.Clear()
	} else if !release.IsZero() {
		repository, err := etcd.NewScriptRepository(fixture.store)
		if err != nil {
			t.Fatal(err)
		}
		scripts, err = repository.PrepareBlueprintScriptPublication(ctx, fixture.Environment.Record.ID, fixture.store.revision, task.ID, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer scripts.Clear()
	}
	var publicationStore etcd.EnvironmentBlueprintStore = fixture.store
	if fixture.publicationSize != nil {
		publicationStore = &blueprintSizePublicationStore{
			releasePlanningTestStore: fixture.store,
			audit:                    fixture.publicationSize,
		}
	}
	final, err := etcd.NewEnvironmentBlueprintRepository(publicationStore)
	if err != nil {
		t.Fatal(err)
	}
	result, err := final.PublishEnvironmentBlueprintDesiredRevision(
		ctx,
		netip.Prefix{},
		fixture.Environment.Record.NetworkPool,
		fixture.Project,
		fixture.Environment,
		fixture.head,
		claim,
		testblueprints.EnvironmentDesiredRevisionIdentity{
			EnvironmentID: revision.EnvironmentID,
			RevisionID:    revision.RevisionID,
		},
		projection,
		nil,
		environmentBlueprintTestServiceChanges(t, fixture.store, projection),
		nil,
		groups,
		testcomponentplanning.ComponentTaskPreparation{},
		testblueprintplanning.BlueprintAttachTaskPreparation{},
		testblueprintplanning.BlueprintBackupPolicyPreparation{},
		scripts,
		release,
		etcd.BlueprintRequirementGate{},
		task,
		marker,
	)
	return result, err
}

func environmentBlueprintTestServiceChanges(
	t *testing.T,
	store testkeyvalue.Store,
	projection testenvironmentprojection.EnvironmentComposeProjection,
) []testblueprints.EnvironmentBlueprintServiceChange {
	t.Helper()
	services, err := etcd.NewServiceRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	changes := make([]testblueprints.EnvironmentBlueprintServiceChange, len(projection.DesiredServices))
	for index, desired := range projection.DesiredServices {
		current, err := services.GetService(t.Context(), desired.Desired.ID)
		if err == nil {
			replacement, replaceErr := testservices.ReplaceServiceDesired(current.Record, desired.Desired)
			if replaceErr != nil {
				t.Fatal(replaceErr)
			}
			currentCopy := current
			changes[index] = testblueprints.EnvironmentBlueprintServiceChange{
				Current: &currentCopy,
				Record:  replacement,
			}
			continue
		}
		if !errors.Is(err, errs.New(errs.KindServiceNotFound, "")) {
			t.Fatal(err)
		}
		record, err := testservices.NewServiceRecord(
			projection.EnvironmentID,
			desired.Desired,
			desired.BackingNetworkID,
		)
		if err != nil {
			t.Fatal(err)
		}
		changes[index] = testblueprints.EnvironmentBlueprintServiceChange{Record: record}
	}
	return changes
}
