package etcd

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/oklog/ulid/v2"
)

func TestReleaseScriptNoEffectEvidenceConditionFencesCheckpointRevision(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 9, 4, 18, 0, 0, 0, time.UTC)
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	execution := scriptCheckpointTestRecord(at)
	assignmentID := ids.NewAt(ids.KindAssignment, at, 20)
	value, err := encodeEnvelope("script-execution", execution)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := store.Transact(ctx, []Condition{{Key: scriptExecutionKey(execution.ID)}}, []Mutation{{
		Type: MutationPut, Key: scriptExecutionKey(execution.ID), Value: value,
	}})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed Script execution = %#v, %v", seeded, err)
	}
	task := TaskRecord{
		ID: execution.CurrentTaskID, OperationID: execution.OperationID, Type: TaskDeploy,
		PlanHash: execution.PlanHash,
		Params:   map[string]string{ReleaseHookStepExecutionParam(execution.StepID): execution.ID},
		Steps:    []TaskStepRecord{{Kind: TaskStepScript, ID: execution.StepID}},
	}
	assignment := TaskAssignmentRecord{AssignmentID: assignmentID}
	effect, conditions, err := repository.releaseScriptEffectEvidenceAtRevision(
		ctx, task, assignment, seeded.Revision,
	)
	if err != nil || effect || len(conditions) != 1 || conditions[0].ModRevision != seeded.Revision {
		t.Fatalf("no-effect Script evidence = %t, %#v, %v", effect, conditions, err)
	}
	execution.UpdatedAt = execution.UpdatedAt.Add(time.Nanosecond)
	changedValue, err := encodeEnvelope("script-execution", execution)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: scriptExecutionKey(execution.ID), Value: changedValue,
	}})
	if err != nil || !changed.Succeeded {
		t.Fatalf("change Script checkpoint = %#v, %v", changed, err)
	}
	fenced, err := store.Transact(ctx, conditions, []Mutation{{Type: MutationPut, Key: "/test/no-effect-terminal", Value: []byte("invalid")}})
	if err != nil || fenced.Succeeded {
		t.Fatalf("stale Script checkpoint evidence transaction = %#v, %v", fenced, err)
	}
}

func TestScriptExecutionProjectionSourcesUseImmutableDesiredZones(t *testing.T) {
	at := time.Date(2026, 9, 1, 2, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	revisionID := ids.NewAt(ids.KindTask, at, 2)
	firstID := ids.NewAt(ids.KindNetwork, at, 3)
	secondID := ids.NewAt(ids.KindNetwork, at, 4)
	projection := Versioned[EnvironmentComposeProjection]{
		Record: EnvironmentComposeProjection{
			EnvironmentID: environmentID,
			RevisionID:    revisionID,
			DesiredZones: []EnvironmentZoneProjection{
				{EnvironmentID: environmentID, Desired: testScriptExecutionZone(firstID, "app", environmentID)},
				{EnvironmentID: environmentID, Desired: testScriptExecutionZone(secondID, "data", environmentID)},
			},
		},
		Revision: 17, ReadRevision: 23,
	}
	networks, err := resolveScriptExecutionNetworks(projection)
	if err != nil {
		t.Fatalf("resolveScriptExecutionNetworks() error = %v", err)
	}
	if len(networks) != 2 || networks[0].Record.Desired.ID != firstID ||
		networks[1].Record.Desired.ID != secondID || networks[0].Revision != projection.Revision ||
		networks[1].ReadRevision != projection.ReadRevision {
		t.Fatalf("resolved immutable Zone projections = %#v", networks)
	}

	sources := ScriptExecutionSources{
		Environment:       Versioned[EnvironmentRecord]{Record: EnvironmentRecord{ID: environmentID}},
		DesiredHead:       Versioned[EnvironmentBlueprintHead]{Revision: 13},
		DesiredProjection: projection,
		Networks:          networks,
	}
	conditions := scriptExecutionProjectionConditions(sources)
	wantKeys := []string{
		environmentBlueprintHeadKey(environmentID),
		environmentBlueprintRootKey(environmentID, revisionID),
		deletionTombstoneKey(string(DeletionTargetZone), firstID),
		deletionTombstoneKey(string(DeletionTargetZone), secondID),
	}
	gotKeys := make([]string, len(conditions))
	for index, condition := range conditions {
		gotKeys[index] = condition.Key
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) || conditions[0].ModRevision != 13 ||
		conditions[1].ModRevision != 17 || conditions[2].ModRevision != 0 || conditions[3].ModRevision != 0 {
		t.Fatalf("projection conditions = %#v, want keys %#v", conditions, wantKeys)
	}
}

func testScriptExecutionZone(id string, name string, environmentID string) core.Zone {
	return core.Zone{
		ID: id, Name: name, Subnet: "10.40.0.0/24",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
	}
}

func TestAdvanceScriptExecutionRecordFullCheckpointSequence(t *testing.T) {
	at := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	record := scriptCheckpointTestRecord(at)
	input := scriptCheckpointTestInput(record, at.Add(time.Second))

	input.State = ScriptExecutionStartAuthorized
	input.Evidence = ScriptCheckpointEvidence{
		Kind: ScriptCheckpointEvidenceStartAuthorized, StartAuthorized: &ScriptStartAuthorizedEvidence{},
	}
	record = advanceScriptCheckpointTest(t, record, input)

	input.ExpectedState = record.State
	input.State = ScriptExecutionBodyPrepared
	input.PayloadSHA256 = strings.Repeat("2", 64)
	input.Evidence = ScriptCheckpointEvidence{
		Kind: ScriptCheckpointEvidenceBodyPrepared,
		BodyPrepared: &ScriptBodyPreparedEvidence{
			BodySHA256: record.BodySHA256, UID: 65534, GID: 65534, Device: 10, Inode: 20, Leaf: "body",
		},
	}
	record = advanceScriptCheckpointTest(t, record, input)

	input.ExpectedState = record.State
	input.State = ScriptExecutionContainerCreated
	input.PayloadSHA256 = strings.Repeat("3", 64)
	input.Evidence = ScriptCheckpointEvidence{
		Kind: ScriptCheckpointEvidenceContainerCreated,
		ContainerCreated: &ScriptContainerCreatedEvidence{
			ContainerID: strings.Repeat("a", 64), OwnershipLabelsSHA256: strings.Repeat("b", 64),
		},
	}
	record = advanceScriptCheckpointTest(t, record, input)

	exitCode := int32(0)
	input.ExpectedState = record.State
	input.State = ScriptExecutionOutcomeRecorded
	input.PayloadSHA256 = strings.Repeat("4", 64)
	input.Evidence = ScriptCheckpointEvidence{
		Kind: ScriptCheckpointEvidenceOutcome,
		Outcome: &ScriptOutcomeEvidence{
			Reason: ScriptOutcomeNormalExit, ExitCode: &exitCode, ObservedAt: at.Add(2 * time.Second),
		},
	}
	record = advanceScriptCheckpointTest(t, record, input)

	input.ExpectedState = record.State
	input.State = ScriptExecutionCleanupProven
	input.PayloadSHA256 = strings.Repeat("5", 64)
	input.Evidence = ScriptCheckpointEvidence{
		Kind: ScriptCheckpointEvidenceCleanup,
		Cleanup: &ScriptCleanupEvidence{
			ContainerID: strings.Repeat("a", 64), BodyDevice: 10, BodyInode: 20, BodyLeaf: "body",
			ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true,
		},
	}
	record = advanceScriptCheckpointTest(t, record, input)
	if record.State != ScriptExecutionCleanupProven || record.Cleanup == nil || !record.ActiveReference {
		t.Fatalf("final Script checkpoint = %#v", record)
	}

	input.ExpectedState = ScriptExecutionNotStarted
	input.State = ScriptExecutionStartAuthorized
	input.Evidence = ScriptCheckpointEvidence{
		Kind: ScriptCheckpointEvidenceStartAuthorized, StartAuthorized: &ScriptStartAuthorizedEvidence{},
	}
	if _, err := advanceScriptExecutionRecord(record, input); err == nil {
		t.Fatal("stale Script checkpoint transition error = nil")
	}
}

func advanceScriptCheckpointTest(
	t *testing.T,
	record ScriptExecutionRecord,
	input ScriptCheckpointInput,
) ScriptExecutionRecord {
	t.Helper()
	next, err := advanceScriptExecutionRecord(record, input)
	if err != nil {
		t.Fatalf("advanceScriptExecutionRecord(%s -> %s) error = %v", record.State, input.State, err)
	}
	return next
}

func scriptCheckpointTestInput(record ScriptExecutionRecord, at time.Time) ScriptCheckpointInput {
	return ScriptCheckpointInput{
		TaskID: record.CurrentTaskID, OperationID: record.OperationID,
		AssignmentID: ids.NewAt(ids.KindAssignment, at, 9), AgentID: ids.NewAt(ids.KindAgent, at, 10),
		AgentGeneration: 1, StepID: record.StepID, ExecutionID: record.ID, PlanHash: record.PlanHash,
		ExpectedState: record.State, PayloadSHA256: strings.Repeat("1", 64), At: at,
	}
}

func scriptCheckpointTestRecord(at time.Time) ScriptExecutionRecord {
	return ScriptExecutionRecord{
		ID:          ulid.MustNew(ulid.Timestamp(at), strings.NewReader(strings.Repeat("a", 32))).String(),
		SnapshotID:  ulid.MustNew(ulid.Timestamp(at.Add(time.Millisecond)), strings.NewReader(strings.Repeat("b", 32))).String(),
		OperationID: ids.NewAt(ids.KindOperation, at, 1), CurrentTaskID: ids.NewAt(ids.KindTask, at, 2),
		StepID: ids.NewAt(ids.KindStep, at, 3), ScriptID: ids.NewAt(ids.KindScript, at, 4),
		ScriptGeneration: 1, EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 5),
		ServiceID: ids.NewAt(ids.KindService, at, 6), ReleaseID: ids.NewAt(ids.KindDeployment, at, 7),
		RenderGeneration: 1, PlanHash: strings.Repeat("a", 64), SnapshotSHA256: strings.Repeat("b", 64),
		BodySHA256: strings.Repeat("c", 64), RunnerProjectionSHA256: strings.Repeat("d", 64),
		Plan: []byte{1}, Snapshot: []byte{1}, State: ScriptExecutionNotStarted,
		ActiveReference: true, CreatedAt: at, UpdatedAt: at,
	}
}
