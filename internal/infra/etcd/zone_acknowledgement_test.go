package etcd

import (
	"context"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"testing"
	"time"
)

func TestZoneRemovalCompletionPromotesCandidate(t *testing.T) {
	fixture, tasks := beginAndClaimZoneRemoval(t)
	ctx := context.Background()
	completedAt := fixture.task.CreatedAt.Add(2 * time.Second)
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, fixture.task.ID, testtaskjournal.TaskStatusCompleted, completedAt,
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(completed) error = %v", err)
	}
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, fixture.task.ID, testtaskjournal.TaskStatusCompleted, completedAt,
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(completed replay) error = %v", err)
	}
	current, found, err := fixture.hierarchy.GetEnvironmentComposeProjection(ctx, fixture.environment.Record.ID)
	if err != nil || !found ||
		!testenvironmentchanges.SameServiceRemovalProjection(current.Record, fixture.intent.CandidateProjection) {
		t.Fatalf("completed desired projection = %#v/%v/%v", current, found, err)
	}
	state, err := fixture.store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testenvironmentchanges.ZoneRemovalIntentKey(fixture.intent.OperationID),
				testdeletions.TombstoneKey(string(testdeletions.DeletionTargetZone), fixture.zone.Record.Desired.ID),
				testenvironmentchanges.ComponentTaskActiveEnvironmentKey(fixture.environment.Record.ID),
				testnetworkreservations.ZonePoolRegistryKey(fixture.environment.Record.ID),
			},
		},
	)
	if err != nil || state == nil || len(state.Values) != 4 || state.Values[0] != nil ||
		state.Values[1] != nil || state.Values[2] != nil {
		t.Fatalf("completed Zone removal state = %#v/%v", state, err)
	}
	if state.Values[3] != nil {
		pool, decodeErr := testrecordcodec.Decode[testnetworkreservations.ZonePoolRegistry](
			state.Values[3].Value,
			"zone_pool_registry",
		)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if _, retained := pool.Reservations[fixture.zone.Record.Desired.ID]; retained {
			t.Fatalf("completed Zone retained subnet: %#v", pool)
		}
	}
}

func beginAndClaimZoneRemoval(t *testing.T) (*zoneDeletionProjectionFixture, *TaskRepository) {
	t.Helper()
	fixture := newZoneDeletionProjectionFixture(t, true)
	epochValue, err := testbackupruntime.EncodeEnvironmentMutationEpochRecord(
		testbackupruntime.EnvironmentMutationEpochRecord{
			EnvironmentID: fixture.environment.Record.ID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := fixture.store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type:  testkeyvalue.MutationPut,
		Key:   testhierarchy.EnvironmentMutationEpochKey(fixture.environment.Record.ID),
		Value: epochValue,
	}})
	clear(epochValue)
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed Environment mutation epoch = %#v/%v", seeded, err)
	}
	result, err := fixture.zones.BeginZoneDeletionWithTask(
		context.Background(), fixture.environment, fixture.project, fixture.zone, fixture.authorities,
		fixture.tombstone, fixture.intent, fixture.task, fixture.marker,
	)
	assertZoneDeletionApplied(t, result, err)
	fixture.assertZoneDeletionTombstone(t)
	tasks, err := newTaskRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	claimed, found, err := tasks.ClaimNextControllerTask(
		context.Background(), fixture.task.CreatedAt.Add(time.Second),
	)
	if err != nil || !found || claimed.Task.Record.ID != fixture.task.ID {
		t.Fatalf("ClaimNextControllerTask() = %#v/%v/%v", claimed, found, err)
	}
	return fixture, tasks
}
