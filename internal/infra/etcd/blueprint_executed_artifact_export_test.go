package etcd

import (
	"context"
	"net/netip"
	"testing"

	testblueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ExecutedArtifactFixture exposes only the existing in-memory publisher fixture
// to an external-package test that can use the actual Controller renderer.
type ExecutedArtifactFixture struct {
	Hierarchy   *HierarchyRepository
	Ledger      *ReleaseLedger
	Tasks       *TaskRepository
	Project     testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	Environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	store       *releasePlanningTestStore
	head        int64
	hookScripts BlueprintScriptPublication
}

// buildBlueprintAbsenceAuthorityForTest models an explicitly captured absence
// at one fixed read revision. Applied metadata is an independent fence only.
func buildBlueprintAbsenceAuthorityForTest(
	task TaskRecord,
	predecessor taskMaterializationAppliedPredecessor,
	manifest testreleases.ReleaseStagedManifest,
	applied []byte,
) (testtaskassignments.ReleaseRestorationAuthority, string, error) {
	native := make([]BlueprintNativePredecessor, len(manifest.Members))
	procedure := &agentpb.CandidateReleaseProcedure{
		Members: make([]*agentpb.CandidateReleaseMember, len(manifest.Members)),
	}
	for index, member := range manifest.Members {
		native[index] = BlueprintNativePredecessor{ServiceID: member.ServiceID, FixedReadRevision: 21}
		procedure.Members[index] = &agentpb.CandidateReleaseMember{
			ServiceId: member.ServiceID, CandidateReleaseId: member.ReleaseID,
			CandidateArtifactId: task.Params[testtaskjournal.TaskComposeArtifactParam],
		}
	}
	return buildBlueprintNativeRestorationAuthority(task, predecessor, native, manifest, procedure, applied)
}

func NewExecutedArtifactFixture(t *testing.T) *ExecutedArtifactFixture {
	t.Helper()
	store := &releasePlanningTestStore{memoryHierarchyStore: newMemoryHierarchyStore()}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, hierarchy)
	tasks, err := NewTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewReleaseLedger(store, tasks)
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

func (fixture *ExecutedArtifactFixture) Task(t *testing.T, seed int64) TaskRecord {
	return environmentBlueprintTestTask(t, fixture.Project.Record, fixture.Environment.Record, seed)
}

func (fixture *ExecutedArtifactFixture) Publish(
	t *testing.T,
	task TaskRecord,
	projection testenvironmentprojection.EnvironmentComposeProjection,
	release BlueprintReleasePublication,
) {
	t.Helper()
	result, err := fixture.tryPublish(t, task, projection, release)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("publication outcome=%v conflict=%v err=%v", outcome, conflict, err)
	}
	fixture.head = result.revision
}

func (fixture *ExecutedArtifactFixture) tryPublish(
	t *testing.T,
	task TaskRecord,
	projection testenvironmentprojection.EnvironmentComposeProjection,
	release BlueprintReleasePublication,
) (IdempotencyTransactionResult, error) {
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
		fixture.Hierarchy,
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
	var scripts BlueprintScriptPublication
	if !fixture.hookScripts.IsZero() {
		scripts = fixture.hookScripts
		defer fixture.hookScripts.Clear()
	} else if !release.IsZero() {
		repository, err := newScriptRepository(fixture.store)
		if err != nil {
			t.Fatal(err)
		}
		scripts, err = repository.PrepareBlueprintScriptPublication(ctx, fixture.Environment.Record.ID, fixture.store.revision, task.ID, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer scripts.Clear()
	}
	final, err := newEnvironmentBlueprintRepository(
		fixture.store,
		environmentBlueprintTestTransactionStore{fixture.store},
	)
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
		environmentBlueprintTestServiceChanges(t, fixture.Hierarchy, projection),
		nil,
		groups,
		testcomponentplanning.ComponentTaskPreparation{},
		testblueprintplanning.BlueprintAttachTaskPreparation{},
		testblueprintplanning.BlueprintBackupPolicyPreparation{},
		scripts,
		release,
		BlueprintRequirementGate{},
		task,
		marker,
	)
	return result, err
}
