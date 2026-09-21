package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testbackupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: publication callers must not mutate the durable candidate after fixed-revision derivation.
func TestPreparedBackupRunPublicationRecordIsDefensive(t *testing.T) {
	publication := &PreparedBackupRunPublication{
		state: &preparedBackupRunState{plan: backupRunPublicationPlan{record: testbackupruntime.BackupRunRecord{
			Sources: []testbackupruntime.BackupRunSourceAttemptRecord{
				{
					SourceID: "spt_source",
					Snapshot: testbackupruntime.BackupRunSourceSnapshot{
						Config: &testbackupruntime.BackupConfigSourceSnapshot{ConfigSnapshotID: "cfg_snapshot"},
					},
				},
			},
		}}},
	}
	copy := publication.Record()
	copy.Sources[0].SourceID = "spt_changed"
	copy.Sources[0].Snapshot.Config.ConfigSnapshotID = "cfg_changed"
	if publication.state.plan.record.Sources[0].SourceID != "spt_source" ||
		publication.state.plan.record.Sources[0].Snapshot.Config.ConfigSnapshotID != "cfg_snapshot" {
		t.Fatal("Record exposed mutable publication state")
	}
}

// Rationale: callers must receive a typed failure rather than panic when production composition is absent.
func TestBackupRunPublicationRejectsNilRepository(t *testing.T) {
	var nilOperationContext context.Context
	if _, err := (*BackupRuntimeRepository)(
		nil,
	).PrepareManualBackupRun(nilOperationContext, testbackupplanning.ManualBackupRunInput{}, nil); err == nil {
		t.Fatal("nil repository preparation unexpectedly succeeded")
	}
}

// Rationale: manual preparation must derive the complete run and hierarchy
// owner from one fixed revision rather than accept caller-built records.
func TestPrepareManualBackupRunDerivesFixedRevisionCandidate(t *testing.T) {
	repository, store, seeded := newBackupRuntimeBareFixture(t)
	now := seeded.CreatedAt
	prepared, err := repository.PrepareManualBackupRun(
		context.Background(), testbackupplanning.ManualBackupRunInput{
			EnvironmentID: seeded.EnvironmentID,
			TaskID: ids.NewAt(
				ids.KindTask,
				now,
				980,
			),
			OperationID: ids.NewAt(ids.KindOperation, now, 981),
			PlanID:      ids.NewAt(ids.KindPlan, now, 982),
			FixedRevision: backupRuntimeCurrentRevision(
				t,
				store,
				seeded.EnvironmentID,
			),
			CreatedAt: now,
		}, func(
			_ context.Context,
			_ testkeyvalue.Versioned[testattachments.Record],
			_ testattachments.EncryptedFacts,
			consume func(testbackupplanning.BackupPostgresIdentity) error,
		) error {
			return consume(testbackupplanning.BackupPostgresIdentity{
				Database: seeded.Sources[0].Snapshot.Postgres.Database,
				Role:     seeded.Sources[0].Snapshot.Postgres.Role,
			})
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Publication.Clear()
	if prepared.Run.TaskID == seeded.TaskID || prepared.Run.EnvironmentID != seeded.EnvironmentID ||
		prepared.Owner.EnvironmentID != seeded.EnvironmentID || len(prepared.Run.Sources) == 0 ||
		prepared.Run.Sources[0].SourceID != seeded.Sources[0].SourceID {
		t.Fatalf("prepared manual run = %#v owner=%#v", prepared.Run, prepared.Owner)
	}
}

// Rationale: one derived authority may publish at most once, including after a caller retries locally.
func TestPreparedBackupRunPublicationRejectsSecondPublish(t *testing.T) {
	publication := &PreparedBackupRunPublication{state: &preparedBackupRunState{consumed: true}}
	if _, err := publication.Publish(
		context.Background(), TaskRecord{}, nil, testidempotency.IdempotencyMarker{},
	); err == nil {
		t.Fatal("second Publish unexpectedly succeeded")
	}
}

// Rationale: an existing marker may replay only while every subordinate from
// the winning commit remains present, exact, and at the marker revision.
func TestBackupRunPublicationReplayRejectsSubordinateOmissionAndRewrite(t *testing.T) {
	tests := []struct {
		name   string
		key    func(testbackupruntime.BackupRunRecord, TaskRecord) string
		remove bool
	}{
		{
			name:   "missing run membership",
			remove: true,
			key: func(run testbackupruntime.BackupRunRecord, _ TaskRecord) string {
				key, _ := testbackupruntime.BackupRunEnvironmentIndexKey(run.EnvironmentID, run.TaskID)
				return key
			},
		},
		{name: "rewritten operation index", key: func(_ testbackupruntime.BackupRunRecord, task TaskRecord) string {
			return testtaskjournal.TaskOperationIndexKey(task.OperationID, task.ID)
		}},
		{
			name:   "missing source exclusion",
			remove: true,
			key: func(run testbackupruntime.BackupRunRecord, _ TaskRecord) string {
				exclusions, _ := backupRunExclusionRecords(run, run.CreatedAt)
				key, _ := testbackupruntime.BackupSourceTargetExclusionKey(
					exclusions[0].TargetKind,
					exclusions[0].TargetID,
				)
				return key
			},
		},
		{name: "rewritten owner index", key: func(_ testbackupruntime.BackupRunRecord, task TaskRecord) string {
			keys, _ := taskOwnerIndexKeys(task.Owner, task.ID)
			return keys[0]
		}},
		{name: "rewritten mutation epoch", key: func(run testbackupruntime.BackupRunRecord, _ TaskRecord) string {
			return testhierarchy.EnvironmentMutationEpochKey(run.EnvironmentID)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository, store, run, task, marker, commitRevision := publishBackupRunReplayFixture(t)
			if err := repository.validateExistingBackupRunPublication(
				context.Background(), marker, commitRevision, commitRevision,
			); err != nil {
				t.Fatalf("initial replay validation = %v", err)
			}
			key := test.key(run, task)
			entry := mustOptionalKey(t, store, key)
			mutation := testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: []byte("rewritten")}
			if test.remove {
				mutation = testkeyvalue.Mutation{Type: testkeyvalue.MutationDelete, Key: key}
			}
			changed, err := store.Transact(
				context.Background(),
				[]testkeyvalue.Condition{{Key: key, ModRevision: entry.ModRevision}},
				[]testkeyvalue.Mutation{mutation},
			)
			if err != nil || !changed.Succeeded {
				t.Fatalf("tamper subordinate = %#v, %v", changed, err)
			}
			if err := repository.validateExistingBackupRunPublication(
				context.Background(), marker, changed.Revision, commitRevision,
			); err == nil {
				t.Fatal("tampered subordinate unexpectedly replayed")
			}
		})
	}
}

// Rationale: a losing request may observe the winner only after an Agent has
// replaced queue membership with its assignment; that coherent successor is
// still the same pending operation and must classify as in progress.
func TestBackupRunPublicationReplayAcceptsClaimedWinner(t *testing.T) {
	repository, store, run, _, marker, _ := publishBackupRunReplayFixture(t)
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 7101)
	claim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != run.TaskID {
		t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, err)
	}
	result, err := applyCompetingBackupRunPublication(
		t, repository, store, marker, ids.NewAt(ids.KindTask, run.CreatedAt, 7102),
	)
	if err != nil {
		t.Fatalf("Apply(loser after claim) error = %v", err)
	}
	outcome, existing, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownExisting ||
		existing.State != testidempotency.IdempotencyMarkerPending || existing.TaskID != run.TaskID {
		t.Fatalf("claimed winner classification = %v/%#v/%v/%v", outcome, existing, conflict, err)
	}
}

// Rationale: a terminal Backup receipt replaces transient lock, queue, and
// assignment evidence, so a loser resolving afterward must replay the winner's
// terminal marker rather than require deleted pending authority.
func TestBackupRunPublicationReplayAcceptsTerminalWinner(t *testing.T) {
	repository, store, run, _, marker, _ := publishBackupRunReplayFixture(t)
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 7201)
	claim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(time.Second),
	)
	if err != nil || !found {
		t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, err)
	}
	resultRecord := completedComposeTaskResult()
	resultRecord.ExitCode = 1
	terminal, err := tasks.AcknowledgeTask(
		context.Background(),
		agentID,
		1,
		run.TaskID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, resultRecord,
		run.CreatedAt.Add(2*time.Second),
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusFailed {
		t.Fatalf("AcknowledgeTask() = %#v/%v", terminal, err)
	}
	result, err := applyCompetingBackupRunPublication(
		t, repository, store, marker, ids.NewAt(ids.KindTask, run.CreatedAt, 7202),
	)
	if err != nil {
		t.Fatalf("Apply(loser after terminal) error = %v", err)
	}
	outcome, existing, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownExisting ||
		existing.State != testidempotency.IdempotencyMarkerFailed || existing.TaskID != run.TaskID {
		t.Fatalf("terminal winner classification = %v/%#v/%v/%v", outcome, existing, conflict, err)
	}
}

// Rationale: successor recognition is fail closed: deleting any assignment
// copy or the terminal receipt creates a torn lifecycle, not an idempotent
// in-progress or replay result.
func TestBackupRunPublicationReplayRejectsTornSuccessors(t *testing.T) {
	t.Run("claimed assignment index missing", func(t *testing.T) {
		repository, store, run, _, marker, publicationRevision := publishBackupRunReplayFixture(t)
		tasks, err := newTaskRepository(store)
		if err != nil {
			t.Fatal(err)
		}
		claim, found, err := tasks.ClaimNextTask(
			context.Background(),
			ids.NewAt(ids.KindAgent, run.CreatedAt, 7301),
			1,
			run.CreatedAt.Add(time.Second),
		)
		if err != nil || !found {
			t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, err)
		}
		torn, err := store.Transact(
			context.Background(),
			nil,
			[]testkeyvalue.Mutation{
				{Type: testkeyvalue.MutationDelete, Key: testtaskjournal.TaskAssignmentIndexKey(run.TaskID)},
			},
		)
		if err != nil || !torn.Succeeded {
			t.Fatalf("delete assignment index = %#v/%v", torn, err)
		}
		if err := repository.validateExistingBackupRunPublication(
			context.Background(), marker, torn.Revision, publicationRevision,
		); err == nil {
			t.Fatal("torn claimed winner unexpectedly validated")
		}
	})

	t.Run("terminal receipt missing", func(t *testing.T) {
		repository, store, run, _, marker, _ := publishBackupRunReplayFixture(t)
		tasks, err := newTaskRepository(store)
		if err != nil {
			t.Fatal(err)
		}
		agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 7401)
		claim, found, err := tasks.ClaimNextTask(
			context.Background(), agentID, 1, run.CreatedAt.Add(time.Second),
		)
		if err != nil || !found {
			t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, err)
		}
		resultRecord := completedComposeTaskResult()
		resultRecord.ExitCode = 1
		if _, err := tasks.AcknowledgeTask(
			context.Background(), agentID, 1, run.TaskID,
			claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, resultRecord,
			run.CreatedAt.Add(2*time.Second),
		); err != nil {
			t.Fatal(err)
		}
		terminalMarker, terminalRevision := storedBackupRunMarker(t, store, marker.Locator)
		torn, err := store.Transact(
			context.Background(),
			nil,
			[]testkeyvalue.Mutation{
				{Type: testkeyvalue.MutationDelete, Key: testbackupruntime.BackupTerminalReceiptKey(run.TaskID)},
			},
		)
		if err != nil || !torn.Succeeded {
			t.Fatalf("delete terminal receipt = %#v/%v", torn, err)
		}
		if err := repository.validateExistingBackupRunPublication(
			context.Background(), terminalMarker, torn.Revision, terminalRevision,
		); err == nil {
			t.Fatal("torn terminal winner unexpectedly validated")
		}
	})
}

func applyCompetingBackupRunPublication(
	t *testing.T,
	repository *BackupRuntimeRepository,
	store *memoryHierarchyStore,
	winner testidempotency.IdempotencyMarker,
	losingTaskID string,
) (IdempotencyTransactionResult, error) {
	t.Helper()
	candidate := testidempotency.CloneIdempotencyMarker(winner)
	defer clear(candidate.Intent.Ciphertext)
	defer clear(candidate.Response.Body)
	candidate.TaskID = losingTaskID
	candidate.Response.Body = []byte(`{"task_id":"` + losingTaskID + `"}`)
	plan, err := newIdempotencyMutationPlanForMarker(testidempotency.IdempotencyMarkerTask, nil,
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: "/tests/backup-run-publication/" + losingTaskID,
			Value: []byte("losing-candidate"),
		}},
		func(int64, []*testkeyvalue.KeyValue) error { return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.enforceExistingReplay(repository.validateExistingBackupRunPublication); err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	return idempotency.Apply(context.Background(), candidate, plan)
}

func storedBackupRunMarker(
	t *testing.T,
	store *memoryHierarchyStore,
	locator testidempotency.IdempotencyLocator,
) (testidempotency.IdempotencyMarker, int64) {
	t.Helper()
	key, err := testidempotency.IdempotencyMarkerKey(locator)
	if err != nil {
		t.Fatal(err)
	}
	read, err := store.Get(context.Background(), key)
	if err != nil || read == nil || read.Entry == nil {
		t.Fatalf("Get(marker) = %#v/%v", read, err)
	}
	marker, err := testidempotency.DecodeIdempotencyMarker(read.Entry.Value, locator)
	if err != nil {
		t.Fatal(err)
	}
	return marker, read.Entry.ModRevision
}

// Rationale: the hostile twelve-source distribution must fit only when the
// final Task, owner indexes, marker, and replay target are all counted.
func TestBackupRunPublicationTwelveSourceFinalEnvelopeFitsBounds(t *testing.T) {
	repository, store, run := newBackupRuntimeBareFixture(t)
	for ordinal := uint32(1); ordinal < testbackuppolicy.MaximumBackupPolicySources; ordinal++ {
		source := testBackupLaterSource(run.CreatedAt, ordinal, testbackupruntime.BackupSourceAttemptPending)
		source.Snapshot.Postgres.ConsumerEnvironmentID = run.EnvironmentID
		source.ObjectKey = run.ConnectorPrefix + run.EnvironmentID + "/" + source.SourceID + "/" +
			source.RecoveryPointID + "/artifact.bin"
		run.Sources = append(run.Sources, source)
	}
	extendBackupRuntimePublicationSources(t, store, &run)
	plan, err := repository.prepareBackupRunPublication(
		context.Background(), run, backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.clear()
	task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	publication, err := plan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	conditions := append([]testkeyvalue.Condition(nil), publication.conditions...)
	mutations := append([]testkeyvalue.Mutation(nil), publication.mutations...)
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	markerValue, err := testidempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(markerValue)
	conditions = append(conditions, testkeyvalue.Condition{Key: markerKey})
	mutations = append(
		mutations,
		testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: markerKey, Value: markerValue},
	)
	if marker.ReplayTarget != nil {
		targetKey, keyErr := testidempotency.IdempotencyReplayTargetKey(
			*marker.ReplayTarget, marker.Locator.Method, marker.Locator.Route, marker.Locator.Key,
		)
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		targetValue, encodeErr := testidempotency.EncodeReplayTargetReference(markerKey)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		defer clear(targetValue)
		conditions = append(conditions, testkeyvalue.Condition{Key: targetKey})
		mutations = append(
			mutations, testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: targetKey, Value: targetValue},
		)
	}
	if len(conditions)+len(mutations) > testkeyvalue.MaximumOperations {
		t.Fatalf("final envelope operations = %d", len(conditions)+len(mutations))
	}
	if err := testbackupruntime.ValidateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		t.Fatalf("final envelope bounds = %v", err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idempotency.Apply(context.Background(), marker, publication); err != nil {
		t.Fatalf("Apply(12-source final envelope) = %v", err)
	}
}

func publishBackupRunReplayFixture(
	t *testing.T,
) (*BackupRuntimeRepository, *memoryHierarchyStore, testbackupruntime.BackupRunRecord, TaskRecord, testidempotency.IdempotencyMarker, int64) {
	t.Helper()
	repository, store, run := newBackupRuntimeBareFixture(t)
	plan, err := repository.prepareBackupRunPublication(
		context.Background(), run, backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.clear()
	task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	publication, err := plan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	result, err := idempotency.Apply(context.Background(), marker, publication)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("publication result = %v/%v/%v", outcome, conflict, err)
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	markerEntry := mustOptionalKey(t, store, markerKey)
	return repository, store, run, task, marker, markerEntry.ModRevision
}
