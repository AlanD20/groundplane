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
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: a Controller restart may repeat plan publication, but a newer
// Blueprint head must fence late fact-dependent expansion by the older parent.
func TestPublishBlueprintDesiredPlanReplaysAndFencesSupersededParent(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	now := taskJournalTime()
	parent := validTaskRecord(now)
	parent.Executor = taskjournal.TaskExecutorBlueprint
	parent.Owner.ProjectID = ids.NewAt(ids.KindProject, now, 70)
	parent.Owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, now, 71)
	parent.Target = parent.Owner.EnvironmentID
	parent.Params = map[string]string{blueprints.EnvironmentDesiredRevisionParam: parent.ID}
	parent.Steps = nil
	parent, err = TransitionTaskStatus(
		parent,
		taskjournal.TaskStatusPending,
		taskjournal.TaskStatusRunning,
		now.Add(time.Second),
	)
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
	seed, err := store.Transact(ctx, nil, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(parent.ID), Value: parentValue},
		{Type: keyvalue.MutationPut, Key: taskjournal.BlueprintParentClaimKey(parent.ID), Value: parentRef},
		{
			Type:  keyvalue.MutationPut,
			Key:   blueprints.EnvironmentBlueprintHeadKey(parent.Owner.EnvironmentID),
			Value: parentRef,
		},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed parent = %#v, %v", seed, err)
	}
	attach := blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: ids.NewAt(ids.KindAttach, now, 72)}
	service := blueprintunits.ResourceKey{Kind: ids.KindService, ID: ids.NewAt(ids.KindService, now, 73)}
	incomplete := blueprintunits.DesiredPlan{
		EnvironmentID: parent.Owner.EnvironmentID, ParentTaskID: parent.ID,
		Units: []blueprintunits.Unit{{
			Target: attach, Writes: []blueprintunits.ResourceKey{attach}, Fingerprint: strings.Repeat("a", 64),
		}},
	}
	first, err := repository.PublishBlueprintDesiredPlan(ctx, parent.ID, incomplete)
	if err != nil || first.Revision <= seed.Revision {
		t.Fatalf("publish incomplete plan = %#v, %v", first, err)
	}
	replayed, err := repository.PublishBlueprintDesiredPlan(ctx, parent.ID, incomplete)
	if err != nil || replayed.Revision != first.Revision {
		t.Fatalf("replay exact plan = %#v, %v", replayed, err)
	}
	complete := incomplete
	complete.Complete = true
	complete.Units = append(append([]blueprintunits.Unit(nil), incomplete.Units...), blueprintunits.Unit{
		Target: service, Writes: []blueprintunits.ResourceKey{service}, Fingerprint: strings.Repeat("b", 64),
		After: []blueprintunits.ResourceKey{attach},
	})
	expanded, err := repository.PublishBlueprintDesiredPlan(ctx, parent.ID, complete)
	if err != nil || expanded.Revision <= first.Revision {
		t.Fatalf("seal expanded plan = %#v, %v", expanded, err)
	}
	newHead, err := idempotency.EncodeTaskReference(ids.NewAt(ids.KindTask, now, 74))
	if err != nil {
		t.Fatal(err)
	}
	if tx, err := store.Transact(ctx, nil, []keyvalue.Mutation{{
		Type: keyvalue.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(parent.Owner.EnvironmentID), Value: newHead,
	}}); err != nil || !tx.Succeeded {
		t.Fatalf("supersede head = %#v, %v", tx, err)
	}
	if _, err := repository.PublishBlueprintDesiredPlan(ctx, parent.ID, complete); err == nil {
		t.Fatal("superseded parent published a plan")
	}
}
