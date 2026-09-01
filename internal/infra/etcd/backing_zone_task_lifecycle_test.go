package etcd

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestZoneRemovalFailureAndRetryPreserveBaselineDesiredRevision(t *testing.T) {
	fixture, tasks := beginAndClaimZoneRemoval(t)
	ctx := context.Background()
	failureAt := fixture.task.CreatedAt.Add(2 * time.Second)
	failed, err := tasks.AcknowledgeControllerTask(ctx, fixture.task.ID, TaskStatusFailed, failureAt)
	if err != nil {
		t.Fatalf("AcknowledgeControllerTask(failed) error = %v", err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, fixture.task.ID, TaskStatusFailed, failureAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(failed replay) error = %v", err)
	}
	assertZoneRemovalBaseline(t, fixture, fixture.task.ID, TaskStatusFailed)

	retryAt := failureAt.Add(time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 92)
	wantIntent, err := TransferZoneRemovalIntent(fixture.intent, retryID, retryAt)
	if err != nil {
		t.Fatal(err)
	}
	result, err := tasks.RetryTask(
		ctx, fixture.task.ID, retryID, TaskActorOperator,
		pendingRetryMarker(failed.Record, retryID, retryAt, "zone-removal-retry-key-0001"),
	)
	assertZoneDeletionApplied(t, result, err)
	storedIntent, found, err := fixture.hierarchy.GetZoneRemovalIntent(ctx, fixture.intent.OperationID)
	if err != nil || !found {
		t.Fatalf("GetZoneRemovalIntent(retry) = %#v/%v/%v", storedIntent, found, err)
	}
	wantValue, err := encodeZoneRemovalIntent(wantIntent)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(wantValue)
	gotValue, err := encodeZoneRemovalIntent(storedIntent.Record)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(gotValue)
	if !bytes.Equal(gotValue, wantValue) {
		t.Fatalf("retry intent = %#v, want %#v", storedIntent.Record, wantIntent)
	}
	state, err := fixture.store.GetMany(ctx, GetManyRequest{Keys: []string{
		deletionTombstoneKey(string(DeletionTargetZone), fixture.zone.Record.Desired.ID),
		componentTaskActiveEnvironmentKey(fixture.environment.Record.ID),
	}})
	if err != nil || state == nil || len(state.Values) != 2 || state.Values[0] == nil || state.Values[1] == nil ||
		string(state.Values[1].Value) != retryID {
		t.Fatalf("retry fences = %#v/%v", state, err)
	}
	tombstone, err := decodeDeletionTombstone(state.Values[0].Value)
	if err != nil || tombstone.TaskID != retryID || tombstone.TargetRevision != fixture.intent.ZoneRevision {
		t.Fatalf("retry tombstone = %#v/%v", tombstone, err)
	}

	abortAt := retryAt.Add(time.Second)
	if _, err := tasks.AbortPendingTask(ctx, retryID, abortAt); err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	if _, err := tasks.AbortPendingTask(ctx, retryID, abortAt); err != nil {
		t.Fatalf("AbortPendingTask(replay) error = %v", err)
	}
	assertZoneRemovalBaseline(t, fixture, retryID, TaskStatusAborted)
}

func assertZoneRemovalBaseline(
	t *testing.T,
	fixture *zoneDeletionProjectionFixture,
	taskID string,
	status TaskStatus,
) {
	t.Helper()
	ctx := context.Background()
	current, found, err := fixture.hierarchy.GetEnvironmentComposeProjection(ctx, fixture.environment.Record.ID)
	if err != nil || !found || current.Revision != fixture.projection.Revision ||
		!sameZoneRemovalProjection(current.Record, fixture.projection.Record) {
		t.Fatalf("baseline desired projection = %#v/%v/%v", current, found, err)
	}
	intent, found, err := fixture.hierarchy.GetZoneRemovalIntent(ctx, fixture.intent.OperationID)
	if err != nil || !found || intent.Record.ActiveTaskID != taskID || intent.Record.Status != status ||
		!sameZoneRemovalProjection(intent.Record.DesiredProjection, fixture.projection.Record) ||
		!sameZoneRemovalProjection(intent.Record.AppliedProjection, fixture.authorities.Applied.Record) {
		t.Fatalf("terminal Zone removal intent = %#v/%v/%v", intent, found, err)
	}
	state, err := fixture.store.GetMany(ctx, GetManyRequest{Keys: []string{
		deletionTombstoneKey(string(DeletionTargetZone), fixture.zone.Record.Desired.ID),
		componentTaskActiveEnvironmentKey(fixture.environment.Record.ID),
		zonePoolRegistryKey(fixture.environment.Record.ID),
	}})
	if err != nil || state == nil || len(state.Values) != 3 || state.Values[0] != nil ||
		state.Values[1] != nil || state.Values[2] == nil {
		t.Fatalf("terminal baseline state = %#v/%v", state, err)
	}
	pool, err := decodeEnvelope[zonePoolRegistry](state.Values[2].Value, "zone_pool_registry")
	if err != nil || pool.Reservations[fixture.zone.Record.Desired.ID] != fixture.zone.Record.Desired.Subnet {
		t.Fatalf("terminal Zone pool = %#v/%v", pool, err)
	}
}
