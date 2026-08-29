package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
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
	record, err := NewProjectSecretRecord(
		secretID, project.Record.ID, "BACKING_PASSWORD", core.SecretKindEnvVar, "", createdAt,
	)
	if err != nil {
		t.Fatalf("NewProjectSecretRecord() error = %v", err)
	}
	created, err := secrets.CreateSecret(
		ctx, ProjectSecretOwner(project), record, testSecretEncryptedValue(secretID, "ciphertext"),
	)
	if err != nil {
		t.Fatalf("CreateSecret() error = %v", err)
	}
	journal, err := newHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatalf("NewHierarchyDeletionRepository() error = %v", err)
	}
	nodes, err := journal.freezeProjectMembership(ctx, HierarchyDeletionOperation{
		Tombstone: HierarchyDeletionTombstone{
			OperationKind:    HierarchyDeletionOperationBacking,
			SnapshotRevision: created.ReadRevision,
		},
	}, project.Record.ID, true)
	if err != nil {
		t.Fatalf("freezeProjectMembership() error = %v", err)
	}
	for _, node := range nodes {
		if node.ActionKind == HierarchyDeletionProjectSecretRemove && node.TargetID == secretID {
			return
		}
	}
	t.Fatalf("backing credential Secret %s missing from frozen membership: %#v", secretID, nodes)
}

func TestEnvironmentDeletionFreezesAndFinalizesReleaseGroups(t *testing.T) {
	// Rationale: Environment deletion must remove Release Group ownership before
	// Service removal and parent finalization can prove the Environment is empty.
	ctx := context.Background()
	fixture := newReleaseGroupPublisherFixture(t, 2)
	journal, err := newHierarchyDeletionRepository(fixture.store)
	if err != nil {
		t.Fatalf("newHierarchyDeletionRepository() error = %v", err)
	}
	primary, err := fixture.store.Get(ctx, environmentKey(fixture.environment.Record.ID))
	if err != nil || primary == nil || primary.Entry == nil {
		t.Fatalf("Environment primary = %#v, %v", primary, err)
	}
	operation := HierarchyDeletionOperation{Tombstone: HierarchyDeletionTombstone{
		OperationID:      ids.NewAt(ids.KindOperation, fixture.now, 970),
		SnapshotRevision: fixture.store.revision,
	}}
	nodes, err := journal.freezeEnvironmentMembership(
		ctx,
		operation,
		fixture.environment.Record.ID,
		primary.Entry.ModRevision,
		hierarchyDeletionBytesDigest(primary.Entry.Value),
	)
	if err != nil {
		t.Fatalf("freezeEnvironmentMembership() error = %v", err)
	}
	var groupNode *HierarchyDeletionMembershipNode
	for index := range nodes {
		if nodes[index].ActionKind == HierarchyDeletionReleaseGroupRemove &&
			nodes[index].TargetID == fixture.group.ID {
			groupNode = &nodes[index]
			break
		}
	}
	if groupNode == nil || groupNode.ProcedureInput.ControllerFinalizer == nil ||
		groupNode.ProcedureInput.ControllerFinalizer.Finalizer != "release-group.remove" {
		t.Fatalf("Release Group deletion node = %#v", groupNode)
	}
	effects, err := journal.prepareHierarchyDeletionControllerEffects(ctx, operation, HierarchyDeletionAction{
		ActionKind:     groupNode.ActionKind,
		TargetKind:     groupNode.TargetKind,
		TargetID:       groupNode.TargetID,
		TargetRevision: groupNode.TargetRevision,
	})
	if err != nil {
		t.Fatalf("prepareHierarchyDeletionControllerEffects() error = %v", err)
	}
	transaction, err := fixture.store.Transact(ctx, effects.conditions, effects.mutations)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("Release Group finalizer transaction = %#v, %v", transaction, err)
	}
	for _, key := range []string{
		releaseGroupRecordKey(fixture.group.ID),
		releaseGroupOwnerKey(fixture.group.EnvironmentID, fixture.group.ID),
		releaseGroupNameKey(fixture.group.EnvironmentID, fixture.group.Name),
	} {
		if value := fixture.store.valueAt(key, fixture.store.revision); value != nil {
			t.Fatalf("Release Group finalizer retained %s", key)
		}
	}
	if value := fixture.store.valueAt(
		releaseGroupCollectionEpochKey(fixture.group.EnvironmentID),
		fixture.store.revision,
	); value == nil {
		t.Fatal("Release Group child finalizer removed the parent-owned collection epoch")
	}
}
