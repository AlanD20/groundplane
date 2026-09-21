package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testhierarchydeletionfinalization "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionfinalization"
	testhierarchydeletionplanning "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionplanning"
	testreleasegroups "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
)

func TestBackingDeletionFreezesProjectSecrets(t *testing.T) {
	// Rationale: backing-service deletion owns its generated credential Secret and must plan
	// that Secret before the Project facade finalizer verifies descendant ownership is empty.
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	backing := seedBackingService(t, store, 960, "deletion-cache")
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, err := hierarchy.GetProject(ctx, backing.ProjectID)
	if err != nil {
		t.Fatalf("GetProject() error = %v", err)
	}
	secrets, err := newSecretRepository(store)
	if err != nil {
		t.Fatalf("newSecretRepository() error = %v", err)
	}
	createdAt := time.Date(2026, 8, 29, 9, 0, 0, 0, time.UTC)
	secretID := ids.NewAt(ids.KindSecret, createdAt, 962)
	record, err := testsecrets.NewProjectRecord(
		secretID, project.Record.ID, "BACKING_PASSWORD", core.SecretKindEnvVar, "", createdAt,
	)
	if err != nil {
		t.Fatalf("NewProjectSecretRecord() error = %v", err)
	}
	created, err := secrets.CreateSecret(
		ctx, testsecrets.ProjectOwner(project), record, testSecretEncryptedValue(secretID, "ciphertext"),
	)
	if err != nil {
		t.Fatalf("CreateSecret() error = %v", err)
	}
	tombstone := testhierarchydeletion.HierarchyDeletionTombstone{
		OperationID:   ids.NewAt(ids.KindOperation, createdAt, 963),
		OperationKind: testhierarchydeletion.HierarchyDeletionOperationBacking,
		TargetKind:    testhierarchydeletion.HierarchyDeletionTargetBacking,
		TargetID:      project.Record.ID, TargetRevision: project.Revision,
		SnapshotRevision: created.ReadRevision, DeletionEpoch: 1,
	}
	frozen, err := testhierarchydeletionplanning.NewPlanner(store).FreezeCapturedMembership(ctx, tombstone)
	if err != nil {
		t.Fatalf("FreezeCapturedMembership() error = %v", err)
	}
	for _, node := range frozen.Nodes {
		if node.ActionKind == testhierarchydeletion.HierarchyDeletionProjectSecretRemove && node.TargetID == secretID {
			return
		}
	}
	t.Fatalf("backing credential Secret %s missing from frozen membership: %#v", secretID, frozen.Nodes)
}

func TestEnvironmentDeletionFreezesAndFinalizesReleaseGroups(t *testing.T) {
	// Rationale: Environment deletion must remove Release Group ownership before
	// Service removal and parent finalization can prove the Environment is empty.
	ctx := context.Background()
	fixture := newReleaseGroupPublisherFixture(t, 2)
	primary, err := fixture.store.Get(ctx, testhierarchy.EnvironmentKey(fixture.environment.Record.ID))
	if err != nil || primary == nil || primary.Entry == nil {
		t.Fatalf("Environment primary = %#v, %v", primary, err)
	}
	operation := testhierarchydeletion.HierarchyDeletionOperation{
		Tombstone: testhierarchydeletion.HierarchyDeletionTombstone{
			OperationID:   ids.NewAt(ids.KindOperation, fixture.now, 970),
			OperationKind: testhierarchydeletion.HierarchyDeletionOperationEnvironment,
			TargetKind:    testhierarchydeletion.HierarchyDeletionTargetEnvironment,
			TargetID:      fixture.environment.Record.ID, TargetRevision: primary.Entry.ModRevision,
			SnapshotRevision: fixture.store.revision, DeletionEpoch: 1,
		},
	}
	frozen, err := testhierarchydeletionplanning.NewPlanner(fixture.store).
		FreezeCapturedMembership(ctx, operation.Tombstone)
	if err != nil {
		t.Fatalf("FreezeCapturedMembership() error = %v", err)
	}
	nodes := frozen.Nodes
	var groupNode *testhierarchydeletionplanning.HierarchyDeletionMembershipNode
	for index := range nodes {
		if nodes[index].ActionKind == testhierarchydeletion.HierarchyDeletionReleaseGroupRemove &&
			nodes[index].TargetID == fixture.group.ID {
			groupNode = &nodes[index]
			break
		}
	}
	if groupNode == nil || groupNode.ProcedureInput.ControllerFinalizer == nil ||
		groupNode.ProcedureInput.ControllerFinalizer.Finalizer != "release-group.remove" {
		t.Fatalf("Release Group deletion node = %#v", groupNode)
	}
	effects, err := testhierarchydeletionfinalization.NewPreparer(fixture.store).
		Prepare(ctx, operation, testhierarchydeletion.HierarchyDeletionAction{
			ActionKind:     groupNode.ActionKind,
			TargetKind:     groupNode.TargetKind,
			TargetID:       groupNode.TargetID,
			TargetRevision: groupNode.TargetRevision,
		})
	if err != nil {
		t.Fatalf("prepareHierarchyDeletionControllerEffects() error = %v", err)
	}
	transaction, err := fixture.store.Transact(ctx, effects.Conditions(), effects.Mutations())
	if err != nil || !transaction.Succeeded {
		t.Fatalf("Release Group finalizer transaction = %#v, %v", transaction, err)
	}
	for _, key := range []string{testreleasegroups.ReleaseGroupRecordKey(fixture.group.ID), testreleasegroups.ReleaseGroupOwnerKey(fixture.group.EnvironmentID, fixture.group.ID), testreleasegroups.ReleaseGroupNameKey(fixture.group.EnvironmentID, fixture.group.Name)} {
		if value := fixture.store.valueAt(key, fixture.store.revision); value != nil {
			t.Fatalf("Release Group finalizer retained %s", key)
		}
	}
	if value := fixture.store.valueAt(testreleasegroups.ReleaseGroupCollectionEpochKey(fixture.group.EnvironmentID), fixture.store.revision); value == nil {
		t.Fatal("Release Group child finalizer removed the parent-owned collection epoch")
	}
}
