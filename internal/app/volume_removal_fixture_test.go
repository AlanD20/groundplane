package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// volumeRemovalProductionFixture exposes real publication repositories over
// the existing MVCC fake. It does not fabricate a removal Task or runtime.
type volumeRemovalProductionFixture struct {
	Blueprint     *etcd.EnvironmentBlueprintRepository
	Idempotency   *etcd.IdempotencyRepository
	Tasks         *etcd.TaskRepository
	Store         testkeyvalue.Store
	Evidence      *volumeEvidenceStageAudit
	EnvironmentID string
	reserved      []testscriptsourceevidence.ScriptSourcePreparationMember
	authority     *testscriptsourcepublication.Authority
}

func (fixture *ExecutedArtifactFixture) VolumeMutationFixture(t *testing.T) *volumeRemovalProductionFixture {
	t.Helper()
	blueprint, err := etcd.NewEnvironmentBlueprintRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := etcd.NewIdempotencyRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	return &volumeRemovalProductionFixture{
		Blueprint: blueprint, Idempotency: idempotency, Tasks: fixture.Tasks, Store: fixture.store,
		Evidence: newVolumeEvidenceStageAudit(fixture.store), EnvironmentID: fixture.Environment.Record.ID,
	}
}

func newVolumeRemovalProductionFixture(t *testing.T) *volumeRemovalProductionFixture {
	t.Helper()
	store := &releasePlanningTestStore{memoryHierarchyStore: newMemoryHierarchyStore()}
	blueprint, err := etcd.NewEnvironmentBlueprintRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	_, environment := createEnvironmentBlueprintOwners(t, blueprint.HierarchyRepository)
	idempotency, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := testscriptsourcepublication.NewAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	return &volumeRemovalProductionFixture{
		Blueprint: blueprint, Idempotency: idempotency, Tasks: tasks, Store: store, Evidence: newVolumeEvidenceStageAudit(store),
		EnvironmentID: environment.Record.ID, authority: authority,
	}
}

// CompleteCreate supplies hermetic Agent evidence through the real claim and
// acknowledgement path; no filesystem readiness is inferred by the fixture.
func (fixture *volumeRemovalProductionFixture) CompleteCreate(t *testing.T, taskID string) {
	t.Helper()
	ctx := context.Background()
	task, err := fixture.Tasks.GetTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.New(ids.KindAgent)
	assignment, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, task.Record.CreatedAt.Add(time.Millisecond))
	if err != nil || !found || assignment.Task.Record.ID != taskID {
		t.Fatalf("claim Volume creation: found=%t error=%v", found, err)
	}
	_, err = fixture.Tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		taskID,
		assignment.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		testtaskjournal.TaskResultRecord{
			Kind:       testtaskjournal.TaskResultEnvironmentDirectory,
			Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
		},
		task.Record.CreatedAt.Add(2*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("complete Volume creation: %v", err)
	}
}

func (fixture *volumeRemovalProductionFixture) ReserveVolume(t *testing.T, volumeID string) {
	t.Helper()
	ctx := context.Background()
	executionID, snapshotID := ids.NewULID(), ids.NewULID()
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(&agentpb.ResolvedRunnerSnapshot{
		SnapshotId: snapshotID, ScriptExecutionId: executionID, EnvironmentId: fixture.EnvironmentID,
		Mounts: []*agentpb.ScriptRunnerMount{{SourceId: volumeID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	value, err := testrecordcodec.Encode("script-runner-snapshot", testscriptsourceevidence.StoredScriptRunnerSnapshot{
		ExecutionID: executionID, SnapshotID: snapshotID, SHA256: hex.EncodeToString(digest[:]), Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	key := testscriptexecutions.ScriptRunnerSnapshotKey(snapshotID)
	revision, err := fixture.Store.Put(ctx, key, value)
	if err != nil {
		t.Fatal(err)
	}
	fixture.reserved = []testscriptsourceevidence.ScriptSourcePreparationMember{{
		Reference: testscriptsourcereference.Reference{
			OperationID: ids.New(ids.KindOperation), ScriptExecutionID: executionID,
			Source: testscriptsourcereference.SourceIdentity{
				Kind:     testscriptsourcereference.SourceVolume,
				VolumeID: volumeID,
			},
			SourceOwnerID: fixture.EnvironmentID, SourceModRevision: revision, SourceDigest: hex.EncodeToString(digest[:]),
		},
		Evidence: testscriptsourceevidence.ScriptSourceEvidence{
			Existing: &testscriptsourceevidence.ScriptExistingSourceEvidence{SourceKey: key},
		},
	}}
	if _, err := fixture.authority.Prepare(ctx, fixture.reserved[0].Reference.OperationID, fixture.reserved); err != nil {
		t.Fatal(err)
	}
}

func (fixture *volumeRemovalProductionFixture) ReleaseVolume(t *testing.T) {
	t.Helper()
	if err := fixture.authority.Abandon(context.Background(), fixture.reserved[0].Reference.OperationID, fixture.reserved); err != nil {
		t.Fatal(err)
	}
}
