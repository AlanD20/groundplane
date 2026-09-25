package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: a private Agent child must be published with its durable unit
// claim and lifecycle marker, so it can neither duplicate a ready effect nor
// become an unfinishable running Task.
func TestPublishBlueprintChildClaimsOneReadyUnitWithLifecycle(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := taskJournalTime()
	parent := validTaskRecord(now)
	parent.Executor = taskjournal.TaskExecutorBlueprint
	parent.Owner.ProjectID = ids.NewAt(ids.KindProject, now, 80)
	parent.Owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, now, 81)
	parent.Target = parent.Owner.EnvironmentID
	parent.Params = map[string]string{blueprints.EnvironmentDesiredRevisionParam: parent.ID}
	parent.Steps = nil
	parentMarker := pendingTaskMarker(parent)
	parentMarker.Locator.ScopeID = parent.Owner.EnvironmentID
	parent.idempotencyMarker = cloneIdempotencyLocator(&parentMarker.Locator)
	parent, err = TransitionTaskStatus(
		parent,
		taskjournal.TaskStatusPending,
		taskjournal.TaskStatusRunning,
		now.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	parentMarkerValue, err := idempotency.EncodeIdempotencyMarker(parentMarker)
	if err != nil {
		t.Fatal(err)
	}
	parentMarkerKey, err := idempotency.IdempotencyMarkerKey(parentMarker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	parentValue, err := EncodeTaskRecord(parent)
	if err != nil {
		t.Fatal(err)
	}
	parentRef, err := idempotency.EncodeTaskReference(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	service := blueprintunits.ResourceKey{Kind: ids.KindService, ID: ids.NewAt(ids.KindService, now, 82)}
	unit := blueprintunits.Unit{
		Target:      service,
		Fingerprint: strings.Repeat("b", 64),
		Writes:      []blueprintunits.ResourceKey{service},
	}
	plan, err := blueprintunits.EncodeDesiredPlan(blueprintunits.DesiredPlan{
		EnvironmentID: parent.Owner.EnvironmentID, ParentTaskID: parent.ID, Complete: true,
		Units: []blueprintunits.Unit{unit},
	})
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := blueprintunits.EncodeEpoch(
		blueprintunits.EpochRecord{EnvironmentID: parent.Owner.EnvironmentID, Sequence: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	initialAbsence, err := blueprintunits.PrepareInitialAbsencePublication(
		parent.Owner.EnvironmentID, parent.ID, []blueprintunits.ResourceKey{service},
	)
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := store.Transact(ctx, initialAbsence.Conditions(), append([]keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(parent.ID), Value: parentValue},
		{Type: keyvalue.MutationPut, Key: taskjournal.BlueprintParentClaimKey(parent.ID), Value: parentRef},
		{Type: keyvalue.MutationPut, Key: parentMarkerKey, Value: parentMarkerValue},
		{Type: keyvalue.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(parent.Owner.EnvironmentID), Value: parentRef},
		{Type: keyvalue.MutationPut, Key: blueprintunits.DesiredPlanKey(parent.Owner.EnvironmentID), Value: plan},
		{Type: keyvalue.MutationPut, Key: blueprintunits.EpochKey(parent.Owner.EnvironmentID), Value: epoch},
	}, initialAbsence.Mutations()...)); err != nil || !tx.Succeeded {
		t.Fatalf("seed parent and plan = %#v, %v", tx, err)
	}
	child := validTaskRecord(now)
	child.ID = ids.NewAt(ids.KindTask, now, 83)
	child.OperationID = ids.NewAt(ids.KindOperation, now, 84)
	child.PlanID = ids.NewAt(ids.KindPlan, now, 85)
	child.IdempotencyKey = child.ID
	child.Actor = taskjournal.TaskActorSystem
	child.Owner = parent.Owner
	child.Target = parent.Owner.EnvironmentID
	publicationID := ids.NewULID()
	child.Params = map[string]string{
		taskjournal.TaskBlueprintParentParam:       parent.ID,
		blueprints.EnvironmentDesiredRevisionParam: parent.ID,
		releaserender.TaskReleasePublicationParam:  publicationID,
	}
	manifest := releases.ReleaseStagedManifest{
		PublicationID: publicationID, OperationID: child.OperationID, Digest: strings.Repeat("a", 64),
		Members: []releases.ReleaseStagedMemberRef{
			{ServiceID: service.ID, ReleaseID: ids.NewAt(ids.KindDeployment, now, 87)},
		},
		CreatedAt: now,
	}
	manifestValue, err := releases.EncodeReleaseRecord("release-staged-manifest", manifest)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := store.Transact(
		ctx,
		[]keyvalue.Condition{{Key: releases.ReleaseManifestStagingKey(publicationID)}},
		[]keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: releases.ReleaseManifestStagingKey(publicationID), Value: manifestValue},
		},
	)
	if err != nil || !staged.Succeeded {
		t.Fatalf("stage Release manifest = %#v, %v", staged, err)
	}
	publicationValue, err := releases.EncodeReleaseRecord("release-publication", releases.ReleasePublicationMarker{
		PublicationID: publicationID, OperationID: child.OperationID, ManifestDigest: manifest.Digest, PublishedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	publication, err := newBlueprintReleasePublication(blueprintReleasePublicationInput{
		EnvironmentID: child.Owner.EnvironmentID, OperationID: child.OperationID,
		Conditions: []keyvalue.Condition{
			{Key: releases.ReleaseManifestStagingKey(publicationID), ModRevision: staged.Revision},
			{Key: releases.ReleasePublicationKey(publicationID)},
		},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: releases.ReleasePublicationKey(publicationID), Value: publicationValue},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := repository.PublishBlueprintChild(ctx, parent.ID, child, unit, publication)
	if err != nil || created.Record.ID != child.ID {
		t.Fatalf("publish child = %#v, %v", created, err)
	}
	if _, err := repository.PublishBlueprintChild(ctx, parent.ID, child, unit, publication); err == nil {
		t.Fatal("same ready unit was published twice")
	}
	ledger, err := blueprintunits.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := ledger.Load(ctx, parent.Owner.EnvironmentID)
	if err != nil || len(snapshot.Executions) != 1 || snapshot.Executions[0].Record.TaskID != child.ID {
		t.Fatalf("durable child execution = %#v, %v", snapshot.Executions, err)
	}
	markerKey, err := idempotency.IdempotencyMarkerKey(*created.Record.idempotencyMarker)
	if err != nil {
		t.Fatal(err)
	}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
		taskjournal.TaskQueueKey(child.Executor, child.ID), markerKey, releases.ReleasePublicationKey(publicationID),
	}})
	if err != nil || read == nil || len(read.Values) != 3 || read.Values[0] == nil || read.Values[1] == nil ||
		read.Values[2] == nil {
		t.Fatalf("child queue, terminal marker, and Release = %#v, %v", read, err)
	}
	newHead := ids.NewAt(ids.KindTask, now, 86)
	newHeadRef, err := idempotency.EncodeTaskReference(newHead)
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := store.Transact(ctx, nil, []keyvalue.Mutation{{
		Type: keyvalue.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(parent.Owner.EnvironmentID), Value: newHeadRef,
	}}); err != nil || !tx.Succeeded {
		t.Fatalf("supersede desired head = %#v, %v", tx, err)
	}
	aborted, err := repository.AbortPendingTask(ctx, child.ID, now.Add(2*time.Second))
	if err != nil || aborted.Record.Status != taskjournal.TaskStatusAborted {
		t.Fatalf("abort unassigned child = %#v, %v", aborted, err)
	}
	snapshot, err = ledger.Load(ctx, parent.Owner.EnvironmentID)
	if err != nil || len(snapshot.Executions) != 0 {
		t.Fatalf("withdrawn child claim = %#v, %v", snapshot.Executions, err)
	}
}
