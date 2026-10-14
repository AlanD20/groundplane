package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"testing"
	"time"
)

// DNS-05: an ordinary Agent Zone removal must publish its terminal change once,
// then replay the same acknowledgement without duplicate comparison keys.
func TestAgentZoneRemovalCompletionAndReplay(t *testing.T) {
	for _, applied := range []bool{true, false} {
		t.Run(map[bool]string{true: "applied", false: "never applied"}[applied], func(t *testing.T) {
			fixture, tasks := beginZoneRemoval(t, false, applied)
			ctx := context.Background()
			agentID := ids.NewAt(ids.KindAgent, fixture.task.CreatedAt, 99)
			assignment, found, err := tasks.ClaimNextTask(ctx, agentID, 1, fixture.task.CreatedAt.Add(time.Second), 32)
			if err != nil || !found {
				t.Fatalf("ClaimNextTask() = %#v/%v/%v", assignment, found, err)
			}
			for range 2 {
				terminal, err := tasks.AcknowledgeTask(ctx, agentID, 1, fixture.task.ID,
					assignment.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted,
					completedComposeTaskResult(), fixture.task.CreatedAt.Add(2*time.Second))
				if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted {
					t.Fatalf("Agent Zone removal completion = %#v/%v", terminal, err)
				}
			}
			current, found, err := fixture.hierarchy.GetEnvironmentComposeProjection(ctx, fixture.environment.Record.ID)
			if err != nil || !found ||
				!testenvironmentchanges.SameServiceRemovalProjection(
					current.Record,
					fixture.intent.CandidateProjection,
				) {
				t.Fatalf("completed desired projection = %#v/%v/%v", current, found, err)
			}
			for _, key := range []string{
				testenvironmentchanges.ZoneRemovalIntentKey(fixture.intent.OperationID),
				testdeletions.TombstoneKey(string(testdeletions.DeletionTargetZone), fixture.zone.Record.Desired.ID),
			} {
				state, err := fixture.store.Get(ctx, key)
				if err != nil || state.Entry != nil {
					t.Fatalf("retained Zone removal authority = %#v/%v", state, err)
				}
			}
			if !applied {
				_, found, err := fixture.hierarchy.GetEnvironmentAppliedComposeProjection(
					ctx,
					fixture.environment.Record.ID,
				)
				if err != nil || found {
					t.Fatalf("unapplied removal invented applied runtime: found %v, error %v", found, err)
				}
			}
		})
	}
}

func TestZoneRemovalCompletionPromotesCandidate(t *testing.T) {
	fixture, tasks := beginAndClaimZoneRemoval(t)
	ctx := context.Background()
	completedAt := fixture.task.CreatedAt.Add(2 * time.Second)
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, fixture.task.ID, testtaskjournal.TaskStatusCompleted, nil,
		completedAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(completed) error = %v", err)
	}
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, fixture.task.ID, testtaskjournal.TaskStatusCompleted, nil,
		completedAt); err != nil {
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
	fixture, tasks := beginZoneRemoval(t, true, true)
	claimed, found, err := tasks.ClaimNextControllerTask(
		context.Background(), fixture.task.CreatedAt.Add(time.Second),
	)
	if err != nil || !found || claimed.Task.Record.ID != fixture.task.ID {
		t.Fatalf("ClaimNextControllerTask() = %#v/%v/%v", claimed, found, err)
	}
	return fixture, tasks
}

func beginZoneRemoval(t *testing.T, backing, applied bool) (*zoneDeletionProjectionFixture, *TaskRepository) {
	t.Helper()
	fixture := newZoneDeletionProjectionFixture(t, backing)
	if !applied {
		_, err := fixture.store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationDelete,
			Key:  testenvironmentprojection.EnvironmentComposeProjectionStorageKey(fixture.environment.Record.ID),
		}})
		if err != nil {
			t.Fatal(err)
		}
		authorities, found, err := fixture.hierarchy.GetEnvironmentZoneRemovalAuthorities(
			context.Background(),
			fixture.environment.Record.ID,
		)
		if err != nil || !found || authorities.Applied.Revision != 0 {
			t.Fatalf("unapplied authority = %#v/%v/%v", authorities, found, err)
		}
		fixture.authorities = authorities
		fixture.intent.AppliedProjectionRevision = 0
		fixture.intent.AppliedProjection = authorities.Applied.Record
	}
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
	return fixture, tasks
}
