package etcd

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testenvironmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
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
	value, err := testrecordcodec.Encode("script-execution", execution)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := store.Transact(
		ctx,
		[]testkeyvalue.Condition{{Key: testscriptexecutions.ScriptExecutionKey(execution.ID)}},
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testscriptexecutions.ScriptExecutionKey(execution.ID), Value: value,
		}},
	)
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed Script execution = %#v, %v", seeded, err)
	}
	task := TaskRecord{
		ID: execution.CurrentTaskID, OperationID: execution.OperationID, Type: testtaskjournal.TaskDeploy,
		PlanHash: execution.PlanHash,
		Params:   map[string]string{testreleaserender.ReleaseHookStepExecutionParam(execution.StepID): execution.ID},
		Steps:    []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepScript, ID: execution.StepID}},
	}
	assignment := testtaskassignments.TaskAssignmentRecord{AssignmentID: assignmentID}
	effect, conditions, err := repository.releaseScriptEffectEvidenceAtRevision(
		ctx, task, assignment, seeded.Revision,
	)
	if err != nil || effect || len(conditions) != 1 || conditions[0].ModRevision != seeded.Revision {
		t.Fatalf("no-effect Script evidence = %t, %#v, %v", effect, conditions, err)
	}
	execution.UpdatedAt = execution.UpdatedAt.Add(time.Nanosecond)
	changedValue, err := testrecordcodec.Encode("script-execution", execution)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testscriptexecutions.ScriptExecutionKey(execution.ID), Value: changedValue,
	}})
	if err != nil || !changed.Succeeded {
		t.Fatalf("change Script checkpoint = %#v, %v", changed, err)
	}
	fenced, err := store.Transact(
		ctx,
		conditions,
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: "/test/no-effect-terminal", Value: []byte("invalid")},
		},
	)
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
	projection := testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID: environmentID,
			RevisionID:    revisionID,
			DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{
				{EnvironmentID: environmentID, Desired: testScriptExecutionZone(firstID, "app", environmentID)},
				{EnvironmentID: environmentID, Desired: testScriptExecutionZone(secondID, "data", environmentID)},
			},
		},
		Revision: 17, ReadRevision: 23,
	}
	networks := make([]testkeyvalue.Versioned[testzones.Record], 0, len(projection.Record.DesiredZones))
	for _, desired := range projection.Record.DesiredZones {
		network, err := testenvironmentqueries.JoinZone(projection, desired)
		if err != nil {
			t.Fatalf("JoinZone() error = %v", err)
		}
		networks = append(networks, network)
	}
	if len(networks) != 2 || networks[0].Record.Desired.ID != firstID ||
		networks[1].Record.Desired.ID != secondID || networks[0].Revision != projection.Revision ||
		networks[1].ReadRevision != projection.ReadRevision {
		t.Fatalf("resolved immutable Zone projections = %#v", networks)
	}

	sources := testscriptsourcequeries.ScriptExecutionSources{
		Environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{ID: environmentID},
		},
		DesiredHead:       testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead]{Revision: 13},
		DesiredProjection: projection,
		Networks:          networks,
	}
	conditions := scriptExecutionProjectionConditions(sources)
	wantKeys := []string{
		testblueprints.EnvironmentBlueprintHeadKey(environmentID),
		testblueprints.EnvironmentBlueprintRootKey(environmentID, revisionID),
		testdeletions.TombstoneKey(string(testdeletions.DeletionTargetZone), firstID),
		testdeletions.TombstoneKey(string(testdeletions.DeletionTargetZone), secondID),
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

	input.State = testscriptexecutions.ScriptExecutionStartAuthorized
	input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
		Kind: testscriptexecutions.ScriptCheckpointEvidenceStartAuthorized, StartAuthorized: &testscriptexecutions.ScriptStartAuthorizedEvidence{},
	}
	record = advanceScriptCheckpointTest(t, record, input)

	input.ExpectedState = record.State
	input.State = testscriptexecutions.ScriptExecutionBodyPrepared
	input.PayloadSHA256 = strings.Repeat("2", 64)
	input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
		Kind: testscriptexecutions.ScriptCheckpointEvidenceBodyPrepared,
		BodyPrepared: &testscriptexecutions.ScriptBodyPreparedEvidence{
			BodySHA256: record.BodySHA256, UID: 65534, GID: 65534, Device: 10, Inode: 20, Leaf: "body",
		},
	}
	record = advanceScriptCheckpointTest(t, record, input)

	input.ExpectedState = record.State
	input.State = testscriptexecutions.ScriptExecutionContainerCreated
	input.PayloadSHA256 = strings.Repeat("3", 64)
	input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
		Kind: testscriptexecutions.ScriptCheckpointEvidenceContainerCreated,
		ContainerCreated: &testscriptexecutions.ScriptContainerCreatedEvidence{
			ContainerID: strings.Repeat("a", 64), OwnershipLabelsSHA256: strings.Repeat("b", 64),
		},
	}
	record = advanceScriptCheckpointTest(t, record, input)

	exitCode := int32(0)
	input.ExpectedState = record.State
	input.State = testscriptexecutions.ScriptExecutionOutcomeRecorded
	input.PayloadSHA256 = strings.Repeat("4", 64)
	input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
		Kind: testscriptexecutions.ScriptCheckpointEvidenceOutcome,
		Outcome: &testscriptexecutions.ScriptOutcomeEvidence{
			Reason: testscriptexecutions.ScriptOutcomeNormalExit, ExitCode: &exitCode, ObservedAt: at.Add(2 * time.Second),
		},
	}
	record = advanceScriptCheckpointTest(t, record, input)

	input.ExpectedState = record.State
	input.State = testscriptexecutions.ScriptExecutionCleanupProven
	input.PayloadSHA256 = strings.Repeat("5", 64)
	input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
		Kind: testscriptexecutions.ScriptCheckpointEvidenceCleanup,
		Cleanup: &testscriptexecutions.ScriptCleanupEvidence{
			ContainerID: strings.Repeat("a", 64), BodyDevice: 10, BodyInode: 20, BodyLeaf: "body",
			ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true,
		},
	}
	record = advanceScriptCheckpointTest(t, record, input)
	if record.State != testscriptexecutions.ScriptExecutionCleanupProven || record.Cleanup == nil ||
		!record.ActiveReference {
		t.Fatalf("final Script checkpoint = %#v", record)
	}

	input.ExpectedState = testscriptexecutions.ScriptExecutionNotStarted
	input.State = testscriptexecutions.ScriptExecutionStartAuthorized
	input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
		Kind: testscriptexecutions.ScriptCheckpointEvidenceStartAuthorized, StartAuthorized: &testscriptexecutions.ScriptStartAuthorizedEvidence{},
	}
	if _, err := testscriptexecutions.AdvanceScriptExecutionRecord(record, input); err == nil {
		t.Fatal("stale Script checkpoint transition error = nil")
	}
}

func advanceScriptCheckpointTest(
	t *testing.T,
	record testscriptexecutions.ScriptExecutionRecord,
	input testscriptexecutions.ScriptCheckpointInput,
) testscriptexecutions.ScriptExecutionRecord {
	t.Helper()
	next, err := testscriptexecutions.AdvanceScriptExecutionRecord(record, input)
	if err != nil {
		t.Fatalf("advanceScriptExecutionRecord(%s -> %s) error = %v", record.State, input.State, err)
	}
	return next
}

func scriptCheckpointTestInput(
	record testscriptexecutions.ScriptExecutionRecord,
	at time.Time,
) testscriptexecutions.ScriptCheckpointInput {
	return testscriptexecutions.ScriptCheckpointInput{
		TaskID: record.CurrentTaskID, OperationID: record.OperationID,
		AssignmentID: ids.NewAt(ids.KindAssignment, at, 9), AgentID: ids.NewAt(ids.KindAgent, at, 10),
		AgentGeneration: 1, StepID: record.StepID, ExecutionID: record.ID, PlanHash: record.PlanHash,
		ExpectedState: record.State, PayloadSHA256: strings.Repeat("1", 64), At: at,
	}
}

func scriptCheckpointTestRecord(at time.Time) testscriptexecutions.ScriptExecutionRecord {
	return testscriptexecutions.ScriptExecutionRecord{
		ID: ulid.MustNew(ulid.Timestamp(at), strings.NewReader(strings.Repeat("a", 32))).String(),
		SnapshotID: ulid.MustNew(ulid.Timestamp(at.Add(time.Millisecond)), strings.NewReader(strings.Repeat("b", 32))).
			String(),
		OperationID: ids.NewAt(ids.KindOperation, at, 1), CurrentTaskID: ids.NewAt(ids.KindTask, at, 2),
		StepID: ids.NewAt(ids.KindStep, at, 3), ScriptID: ids.NewAt(ids.KindScript, at, 4),
		ScriptGeneration: 1, EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 5),
		ServiceID: ids.NewAt(ids.KindService, at, 6), ReleaseID: ids.NewAt(ids.KindDeployment, at, 7),
		RenderGeneration: 1, PlanHash: strings.Repeat("a", 64), SnapshotSHA256: strings.Repeat("b", 64),
		BodySHA256: strings.Repeat("c", 64), RunnerProjectionSHA256: strings.Repeat("d", 64),
		Plan: []byte{1}, Snapshot: []byte{1}, State: testscriptexecutions.ScriptExecutionNotStarted,
		ActiveReference: true, CreatedAt: at, UpdatedAt: at,
	}
}
