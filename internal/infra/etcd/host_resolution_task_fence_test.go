package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestPlatformDNSResolverTaskFencePublishesAndReplacesActiveTask(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	components, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	records, err := DefaultPlatformComponents(false)
	if err != nil {
		t.Fatalf("DefaultPlatformComponents() error = %v", err)
	}
	now := time.Date(2026, time.August, 30, 2, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, now, 1)
	records[0], err = SetComponentRuntime(records[0], []string{serviceID}, "10.200.0.2", true)
	if err != nil {
		t.Fatalf("SetComponentRuntime() error = %v", err)
	}
	current, err := components.CreatePlatformComponent(ctx, records[0])
	if err != nil {
		t.Fatalf("CreatePlatformComponent() error = %v", err)
	}
	projection, err := NewHostResolutionProjectionRecord(current.ReadRevision, nil)
	if err != nil {
		t.Fatalf("NewHostResolutionProjectionRecord() error = %v", err)
	}
	projectionValue, err := encodeHostResolutionProjectionRecord(projection)
	if err != nil {
		t.Fatalf("encodeHostResolutionProjectionRecord() error = %v", err)
	}
	if result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut, Key: hostResolutionProjectionKey, Value: projectionValue}}); err != nil || !result.Succeeded {
		t.Fatalf("seed projection transaction = %#v, %v", result, err)
	}
	_ = projectionValue
	digest, err := PlatformComponentDesiredDigest(current.Record)
	if err != nil {
		t.Fatalf("PlatformComponentDesiredDigest() error = %v", err)
	}
	tasks.platformResolverTaskPreparer = func(_ context.Context, component Versioned[ComponentRecord], input HostResolutionProjectionRecord, task TaskRecord) (PlatformComponentTaskRenderInput, error) {
		return PlatformComponentTaskRenderInput{
			PlanID: task.PlanID, TaskID: task.ID, ComponentID: component.Record.Desired.ID,
			DesiredSHA256: digest, BaselineGeneration: 1, BaselineSHA256: strings.Repeat("a", 64),
			HostResolutionInputRevision: input.InputRevision, HostResolutionSHA256: input.InputSHA256,
			Config: *component.Record.Desired.Config.CoreDNS, GeneratedServiceID: serviceID,
			DefinitionSHA256: strings.Repeat("b", 64), CatalogSHA256: strings.Repeat("c", 64),
			ActionID: "activate-config", ArtifactID: ids.NewAt(ids.KindConfig, now, 2),
			ComposeArtifactID: ids.NewAt(ids.KindConfig, now, 3), ArtifactSHA256: strings.Repeat("d", 64),
			ArtifactLength: 1, PlanSHA256: strings.Repeat("e", 64),
		}, nil
	}
	first := newPlatformDNSResolverTask(current.Record.Desired.ID, now, "")
	firstChange, err := tasks.preparePlatformDNSResolverTaskContribution(ctx, current, projection, first, nil, nil)
	if err != nil {
		t.Fatalf("prepare first resolver Task = %v", err)
	}
	if result, err := store.Transact(ctx, firstChange.conditions, firstChange.mutations); err != nil || !result.Succeeded {
		t.Fatalf("first resolver Task transaction = %#v, %v", result, err)
	}
	clearHostResolutionReconciliationChange(firstChange)
	activeEntry, err := store.Get(ctx, platformComponentTaskActiveKey(current.Record.Desired.ID))
	if err != nil || activeEntry.Entry == nil {
		t.Fatalf("active resolver Task = %#v, %v", activeEntry, err)
	}
	active := KeyValue{Key: activeEntry.Entry.Key, Value: append([]byte(nil), activeEntry.Entry.Value...), ModRevision: activeEntry.Entry.ModRevision}
	second := newPlatformDNSResolverTask(current.Record.Desired.ID, now.Add(time.Second), first.ID)
	secondChange, err := tasks.preparePlatformDNSResolverTaskContribution(ctx, current, projection, second, &active, nil)
	if err != nil {
		t.Fatalf("prepare successor resolver Task = %v", err)
	}
	if result, err := store.Transact(ctx, secondChange.conditions, secondChange.mutations); err != nil || !result.Succeeded {
		t.Fatalf("successor resolver Task transaction = %#v, %v", result, err)
	}
	clearHostResolutionReconciliationChange(secondChange)
	updated, err := store.Get(ctx, platformComponentTaskActiveKey(current.Record.Desired.ID))
	if err != nil || updated.Entry == nil || string(updated.Entry.Value) != second.ID {
		t.Fatalf("active resolver successor = %#v, %v", updated, err)
	}
	if firstRead, err := store.Get(ctx, taskKey(first.ID)); err != nil || firstRead.Entry == nil {
		t.Fatalf("first resolver Task disappeared = %#v, %v", firstRead, err)
	}
	if secondRead, err := store.Get(ctx, taskKey(second.ID)); err != nil || secondRead.Entry == nil {
		t.Fatalf("successor resolver Task missing = %#v, %v", secondRead, err)
	}
	if _, err := store.Transact(ctx, []Condition{{
		Key: platformComponentTaskActiveKey(current.Record.Desired.ID), ModRevision: updated.Entry.ModRevision,
	}}, []Mutation{{Type: MutationDelete, Key: platformComponentTaskActiveKey(current.Record.Desired.ID)}}); err != nil {
		t.Fatalf("clear terminal resolver fence: %v", err)
	}
	sourceRead, err := store.Get(ctx, taskKey(second.ID))
	if err != nil || sourceRead.Entry == nil {
		t.Fatalf("retry source resolver Task = %#v, %v", sourceRead, err)
	}
	source, err := decodeTaskRecord(sourceRead.Entry.Value)
	if err != nil {
		t.Fatalf("decode retry source resolver Task: %v", err)
	}
	retry := cloneTaskRecord(source)
	retry.ID = ids.NewAt(ids.KindTask, now, 4)
	retry.RetryOf = source.ID
	retry.Actor = TaskActorOperator
	retryChange, err := tasks.preparePlatformDNSResolverTaskRetry(ctx, Versioned[TaskRecord]{
		Record: source, Revision: sourceRead.Entry.ModRevision, ReadRevision: sourceRead.ReadRevision,
	}, retry)
	if err != nil || !retryChange.applies {
		t.Fatalf("prepare resolver retry = %#v, %v", retryChange, err)
	}
	if result, err := store.Transact(ctx, retryChange.conditions, retryChange.mutations); err != nil || !result.Succeeded {
		t.Fatalf("resolver retry fence transaction = %#v, %v", result, err)
	}
	clearHostResolutionReconciliationChange(retryChange)
	retried, err := store.Get(ctx, platformComponentTaskActiveKey(current.Record.Desired.ID))
	if err != nil || retried.Entry == nil || string(retried.Entry.Value) != retry.ID {
		t.Fatalf("active resolver retry = %#v, %v", retried, err)
	}
}
