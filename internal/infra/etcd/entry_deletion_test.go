package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testentryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestHierarchyReadsAppliedEntryProjectionFromRuntimeRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, store, environment, _, current, _ := entryDeletionTestState(t)
	want := entryDeletionTestProjection(t, store, current)
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	got, found, err := hierarchy.GetEnvironmentAppliedComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found || got.Revision != want.Revision ||
		got.Record.EnvironmentID != want.Record.EnvironmentID || got.Record.RevisionID != want.Record.RevisionID {
		t.Fatalf(
			"GetEnvironmentAppliedComposeProjection() = %#v, %v, %v; want revision %d",
			got,
			found,
			err,
			want.Revision,
		)
	}
}

// Rationale: a never-applied Entry deletion must atomically publish its Task,
// retain all generations until acknowledgement, then delete them with metadata.
func TestEntryRepositoryCompletesNeverAppliedRemovalAtomically(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, current, generationIDs := entryDeletionTestState(t)
	task, marker, tombstone, intent := entryDeletionTestRecords(t, project, environment, current, nil)
	result, err := repository.BeginEntryDeletionWithTask(
		ctx, environment, project, current, nil, tombstone, intent, task, marker,
	)
	if err != nil {
		t.Fatalf("BeginEntryDeletionWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("BeginEntryDeletionWithTask() outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	assertEntryRemovalRetained(t, repository, store, current, generationIDs)
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(
		ctx, task.CreatedAt.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
	}
	terminalAt := task.CreatedAt.Add(2 * time.Second)
	terminal, err := tasks.AcknowledgeControllerTask(ctx, task.ID, testtaskjournal.TaskStatusCompleted, terminalAt)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("AcknowledgeControllerTask() = %#v/%v", terminal, err)
	}
	if epoch := mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID); epoch != terminal.Revision {
		t.Fatalf("completed Entry deletion epoch = %d, want %d", epoch, terminal.Revision)
	}
	_, err = repository.GetEntry(ctx, current.Record.Entry.ID)
	if !isKind(err, errs.KindEntryNotFound) {
		t.Fatalf("GetEntry(completed removal) error = %v", err)
	}
	for _, key := range []string{testentries.EntryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID), testdeletions.TombstoneKey(string(testdeletions.DeletionTargetEntry), current.Record.Entry.ID)} {
		stored, getErr := store.Get(ctx, key)
		if getErr != nil || stored.Entry != nil {
			t.Fatalf("finalized key %s = %#v/%v", key, stored, getErr)
		}
	}
	assertEntryGenerations(t, store, current.Record.Entry.ID, generationIDs, false)
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	terminalIntent, found, err := hierarchy.GetEntryRemovalIntent(ctx, task.ID)
	if err != nil || !found || terminalIntent.Record.Status != testtaskjournal.TaskStatusCompleted ||
		terminalIntent.Record.TerminalAt == nil || !terminalIntent.Record.TerminalAt.Equal(terminalAt) {
		t.Fatalf("GetEntryRemovalIntent(terminal) = %#v/%v/%v", terminalIntent, found, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, task.ID, testtaskjournal.TaskStatusCompleted, terminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(replay) error = %v", err)
	}
}

// Rationale: a failed removal and its aborted retry must retain the exact
// Entry metadata and every immutable generation while releasing the fence.
func TestEntryRemovalFailureRetryAndAbortRetainState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, current, generationIDs := entryDeletionTestState(t)
	task, marker, tombstone, intent := entryDeletionTestRecords(t, project, environment, current, nil)
	if _, err := repository.BeginEntryDeletionWithTask(
		ctx, environment, project, current, nil, tombstone, intent, task, marker,
	); err != nil {
		t.Fatalf("BeginEntryDeletionWithTask() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(
		ctx, task.CreatedAt.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
	}
	failed, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, testtaskjournal.TaskStatusFailed, task.CreatedAt.Add(2*time.Second),
	)
	if err != nil || failed.Record.Status != testtaskjournal.TaskStatusFailed {
		t.Fatalf("AcknowledgeControllerTask(failed) = %#v/%v", failed, err)
	}
	if epoch := mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID); epoch != failed.Revision {
		t.Fatalf("failed Entry deletion epoch = %d, want %d", epoch, failed.Revision)
	}
	assertEntryRemovalRetained(t, repository, store, current, generationIDs)
	retryAt := task.CreatedAt.Add(3 * time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 9001)
	retryMarker := pendingRetryMarker(failed.Record, retryID, retryAt, "entry-retry-key-0001")
	result, err := tasks.RetryTask(ctx, task.ID, retryID, testtaskjournal.TaskActorOperator, retryMarker)
	if err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryTask() outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	if epoch := mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID); epoch != result.revision {
		t.Fatalf("retried Entry deletion epoch = %d, want %d", epoch, result.revision)
	}
	replay, err := tasks.RetryTask(ctx, task.ID, retryID, testtaskjournal.TaskActorOperator, retryMarker)
	if err != nil {
		t.Fatalf("RetryTask(replay) error = %v", err)
	}
	replayOutcome, _, replayConflict, replayErr := replay.Classify()
	if replayErr != nil || replayConflict != nil || replayOutcome != IdempotencyKnownExisting {
		t.Fatalf("RetryTask(replay) outcome/conflict/error = %v/%v/%v", replayOutcome, replayConflict, replayErr)
	}
	aborted, err := tasks.AbortPendingTask(ctx, retryID, retryAt.Add(time.Second))
	if err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	if epoch := mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID); epoch != aborted.Revision {
		t.Fatalf("aborted Entry deletion epoch = %d, want %d", epoch, aborted.Revision)
	}
	assertEntryRemovalRetained(t, repository, store, current, generationIDs)
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	retryIntent, found, err := hierarchy.GetEntryRemovalIntent(ctx, retryID)
	if err != nil || !found || retryIntent.Record.Status != testtaskjournal.TaskStatusAborted ||
		retryIntent.Record.TerminalAt == nil {
		t.Fatalf("GetEntryRemovalIntent(aborted retry) = %#v/%v/%v", retryIntent, found, err)
	}
}

// Rationale: applied Entry metadata and projection state may advance only
// after the Agent has acknowledged completion of the pinned host cleanup.
func TestEntryRemovalPromotesAppliedProjectionAfterAgentSuccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, current, generationIDs := entryDeletionTestState(t)
	projection := entryDeletionTestProjection(t, store, current)
	task, marker, tombstone, intent := entryDeletionTestRecords(t, project, environment, current, &projection)
	attachRemovalConfiguration(&task, environment.Record.ID)
	projection.ReadRevision = seedTestRuntimeConfigurationHead(
		t, store, environment.Record.ID, projection.Record.RenderGeneration,
	)
	result, err := repository.BeginEntryDeletionWithTask(
		ctx, environment, project, current, &projection, tombstone, intent, task, marker,
	)
	if err != nil {
		t.Fatalf("BeginEntryDeletionWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("BeginEntryDeletionWithTask() outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	agentID := ids.NewAt(ids.KindAgent, task.CreatedAt, 9300)
	if _, found, err := tasks.ClaimNextTask(
		ctx, agentID, 1, task.CreatedAt.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %v/%v", found, err)
	}
	terminalAt := task.CreatedAt.Add(2 * time.Second)
	terminal, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID, taskAssignmentIDForTest(
			t,
			tasks,

			task.ID,
		), testtaskjournal.TaskStatusCompleted, testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone}, terminalAt)

	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("AcknowledgeTask() = %#v/%v", terminal, err)
	}
	published := assertPublishedTaskConfiguration(t, store, task.ID)
	assertRuntimeConfigurationHead(t, store, published, true)
	if epoch := mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID); epoch != terminal.Revision {
		t.Fatalf("completed applied Entry deletion epoch = %d, want %d", epoch, terminal.Revision)
	}
	_, err = repository.GetEntry(ctx, current.Record.Entry.ID)
	if !isKind(err, errs.KindEntryNotFound) {
		t.Fatalf("GetEntry(completed removal) error = %v", err)
	}
	assertEntryGenerations(t, store, current.Record.Entry.ID, generationIDs, false)
	projectionRead, err := store.Get(
		ctx,
		testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environment.Record.ID),
	)
	if err != nil || projectionRead.Entry == nil {
		t.Fatalf("Get(promoted projection) = %#v/%v", projectionRead, err)
	}
	promoted, err := testenvironmentprojection.DecodeEnvironmentComposeProjectionStorage(projectionRead.Entry.Value)
	if err != nil || intent.CandidateProjection == nil ||
		!testenvironmentchanges.SameEntryRemovalProjection(promoted, *intent.CandidateProjection) {
		t.Fatalf("promoted projection = %#v/%v", promoted, err)
	}
	for _, key := range []string{testdeletions.TombstoneKey(string(testdeletions.DeletionTargetEntry), current.Record.Entry.ID), testenvironmentchanges.ComponentTaskActiveEnvironmentKey(environment.Record.ID), testtaskjournal.TaskMaterializationWriterKey(environment.Record.ID)} {
		stored, getErr := store.Get(ctx, key)
		if getErr != nil || stored.Entry != nil {
			t.Fatalf("finalized key %s = %#v/%v", key, stored, getErr)
		}
	}
}

// Rationale: SVC-15/SEC-07 failure releases Entry-removal coordination but
// cannot promote the staged candidate over the last acknowledged files.
func TestEntryRemovalFailureDoesNotPromoteConfiguration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, current, generationIDs := entryDeletionTestState(t)
	projection := entryDeletionTestProjection(t, store, current)
	task, marker, tombstone, intent := entryDeletionTestRecords(t, project, environment, current, &projection)
	attachRemovalConfiguration(&task, environment.Record.ID)
	projection.ReadRevision = seedTestRuntimeConfigurationHead(
		t, store, environment.Record.ID, projection.Record.RenderGeneration,
	)
	result, err := repository.BeginEntryDeletionWithTask(
		ctx, environment, project, current, &projection, tombstone, intent, task, marker,
	)
	if err != nil {
		t.Fatalf("BeginEntryDeletionWithTask() error = %v", err)
	}
	assertAppliedResult(t, result)
	published := assertPublishedTaskConfiguration(t, store, task.ID)
	assertRuntimeConfigurationHead(t, store, published, false)
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.NewAt(ids.KindAgent, task.CreatedAt, 9350)
	if _, found, err := tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %v/%v", found, err)
	}
	terminal, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		taskAssignmentIDForTest(t, tasks, task.ID),
		testtaskjournal.TaskStatusFailed,
		testtaskjournal.TaskResultRecord{
			Kind:       testtaskjournal.TaskResultCompose,
			Diagnostic: testtaskjournal.TaskResultDiagnosticComposeFailed,
			ExitCode:   1,
		},
		task.CreatedAt.Add(2*time.Second),
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusFailed {
		t.Fatalf("AcknowledgeTask(failed) = %#v/%v", terminal, err)
	}
	assertRuntimeConfigurationHead(t, store, published, false)
	assertEntryRemovalRetained(t, repository, store, current, generationIDs)
}

// Rationale: an enabled Cloudflare Tunnel token is a live stable-id reference,
// and even a disabled singleton must remain revision-fenced so a concurrent
// enable or retarget cannot race Entry deletion publication.
// Rationale: an absent Cloudflare singleton is part of the initial Entry
// deletion compare. A singleton created before that compare must produce a
// state conflict without publishing any deletion state.
// Rationale: a failed removal releases its active fence, so retry must read
// and transaction-fence the current Tunnel singleton before reacquiring the
// Entry tombstone. Enabling or retargeting the token to the Entry must block.
// Rationale: a failed removal that originally observed no Cloudflare
// singleton must not reacquire its tombstone when that singleton appears
// during the retry publication compare.
// Rationale: Entry deletion Task publication is an ordinary Environment
// mutation, and replaying its durable marker must not advance the epoch again.
func TestEntryDeletionPublicationFencesLockAndKeepsReplayEpoch(t *testing.T) {
	t.Parallel()
	t.Run("held lock", func(t *testing.T) {
		t.Parallel()
		repository, store, environment, project, current, _ := entryDeletionTestState(t)
		putEnvironmentMutationFenceTestLock(
			t,
			store,
			environment.Record.ID,
			environmentMutationFenceTestOwner(
				time.Date(2026, 8, 24, 22, 0, 0, 0, time.UTC),
				12400,
			),
		)
		task, marker, tombstone, intent := entryDeletionTestRecords(t, project, environment, current, nil)
		if _, err := repository.BeginEntryDeletionWithTask(
			context.Background(),
			environment,
			project,
			current,
			nil,
			tombstone,
			intent,
			task,
			marker,
		); !isKind(err, errs.KindResourceInUse) {
			t.Fatalf("BeginEntryDeletionWithTask() error = %v", err)
		}
	})
	t.Run("replay", func(t *testing.T) {
		t.Parallel()
		repository, store, environment, project, current, _ := entryDeletionTestState(t)
		task, marker, tombstone, intent := entryDeletionTestRecords(t, project, environment, current, nil)
		first, err := repository.BeginEntryDeletionWithTask(
			context.Background(), environment, project, current, nil, tombstone, intent, task, marker,
		)
		if err != nil {
			t.Fatalf("BeginEntryDeletionWithTask() error = %v", err)
		}
		firstOutcome, _, firstConflict, firstClassifyErr := first.Classify()
		if firstClassifyErr != nil || firstConflict != nil || firstOutcome != IdempotencyKnownApplied {
			t.Fatalf("first = %v/%v/%v", firstOutcome, firstConflict, firstClassifyErr)
		}
		afterFirst := mustBackupPolicyMutationEpoch(t, store, environment.Record.ID)
		replay, err := repository.BeginEntryDeletionWithTask(
			context.Background(), environment, project, current, nil, tombstone, intent, task, marker,
		)
		if err != nil {
			t.Fatalf("BeginEntryDeletionWithTask(replay) error = %v", err)
		}
		outcome, _, conflict, classifyErr := replay.Classify()
		if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownExisting {
			t.Fatalf("replay = %v/%v/%v", outcome, conflict, classifyErr)
		}
		afterReplay := mustBackupPolicyMutationEpoch(t, store, environment.Record.ID)
		if afterReplay.Revision != afterFirst.Revision {
			t.Fatalf("replay epoch = %d, want %d", afterReplay.Revision, afterFirst.Revision)
		}
	})
}

func entryDeletionTestState(
	t *testing.T,
) (*EntryRepository, *memoryHierarchyStore, testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], testkeyvalue.Versioned[testhierarchy.ProjectRecord], testkeyvalue.Versioned[testentries.Record], []string) {
	t.Helper()
	_, store, environment, project, _ := routeRepositoryTestHierarchy(t)
	repository, err := newEntryRepository(store)
	if err != nil {
		t.Fatalf("newEntryRepository() error = %v", err)
	}
	now := serviceRecordTestTime().Add(4 * time.Hour)
	entryID := ids.NewAt(ids.KindEnvEntry, now, 9100)
	firstID := ids.NewAt(ids.KindConfig, now, 9101)
	entry := core.EnvEntry{
		ID: entryID, Kind: core.EntryKindEnv, Key: "APP_ENV",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: "staging"}, Exposure: []string{"all"},
	}
	record, err := testentries.NewRecord(environment.Record.ID, entry, firstID)
	if err != nil {
		t.Fatalf("NewEntryRecord() error = %v", err)
	}
	first := testPlainGeneration(environment.Record.ID, entryID, firstID, "staging", now)
	created, err := repository.CreateEntry(
		context.Background(), environment, project, record, testentries.EntryValueGeneration{Plain: &first},
	)
	if err != nil {
		t.Fatalf("CreateEntry() error = %v", err)
	}
	secondID := ids.NewAt(ids.KindConfig, now.Add(time.Second), 9102)
	entry.Source.Literal = "production"
	second := testPlainGeneration(environment.Record.ID, entryID, secondID, "production", now.Add(time.Second))
	current, err := repository.ReplaceEntry(
		context.Background(),
		environment,
		project,
		created,
		entry,
		secondID,
		testentries.EntryValueGeneration{Plain: &second},
	)
	if err != nil {
		t.Fatalf("ReplaceEntry() error = %v", err)
	}
	return repository, store, environment, project, current, []string{firstID, secondID}
}

func entryDeletionTestRecords(
	t *testing.T,
	project testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord],
	entry testkeyvalue.Versioned[testentries.Record],
	projection *testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection],
) (TaskRecord, testidempotency.IdempotencyMarker, testdeletions.DeletionTombstoneRecord, testenvironmentchanges.EntryRemovalIntent) {
	t.Helper()
	createdAt := serviceRecordTestTime().Add(6 * time.Hour)
	task := validTaskRecord(createdAt)
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.ID = ids.NewAt(ids.KindTask, createdAt, 9200)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, 9201)
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, 9202)
	task.Executor = testtaskjournal.TaskExecutorController
	task.Type = testtaskjournal.TaskRemove
	task.Target = entry.Record.Entry.ID
	task.Params = map[string]string{
		testtaskjournal.TaskResourceKindParam:     testtaskjournal.TaskResourceEntry,
		testtaskjournal.TaskEntryEnvironmentParam: entry.Record.EnvironmentID,
	}
	task.TimeoutSeconds = 30
	task.IdempotencyKey = "entry-remove-key-0001"
	if projection != nil {
		task.Executor = testtaskjournal.TaskExecutorAgent
		task.TimeoutSeconds = 120
		task.RenderGeneration = int32(projection.Record.RenderGeneration + 1)
		task.Params = map[string]string{
			testtaskjournal.TaskEntryEnvironmentParam:           entry.Record.EnvironmentID,
			testtaskjournal.TaskMaterializationEnvironmentParam: entry.Record.EnvironmentID,
			testblueprints.EnvironmentDesiredRevisionParam:      projection.Record.RevisionID,
			testtaskjournal.TaskComposeArtifactParam: ids.NewAt(
				ids.KindConfig, createdAt, 9203,
			),
			testtaskjournal.TaskEntryTenantSlugParam:          "tenant",
			testtaskjournal.TaskEntryProjectSlugParam:         project.Record.Slug,
			testtaskjournal.TaskEntryEnvironmentNameParam:     environment.Record.Name,
			testtaskjournal.TaskEntryAuthorizedVolumeDirParam: environment.Record.VolumeDir,
		}
	}
	marker := pendingTaskMarker(task)
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: entry.Record.EnvironmentID,
		Method: http.MethodDelete, Route: "/entries/{id}", Key: task.IdempotencyKey,
	}
	replayTarget := testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetEntry,
		ID:   entry.Record.Entry.ID,
	}
	marker.ReplayTarget = &replayTarget
	intent, err := testenvironmentchanges.NewEntryRemovalIntent(
		task.ID, entry.Record.EnvironmentID, entry.Record.Entry.ID, entry.Revision, projection, createdAt,
	)
	if err != nil {
		t.Fatalf("NewEntryRemovalIntent() error = %v", err)
	}
	tombstone := testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetEntry, TargetID: entry.Record.Entry.ID, TargetRevision: entry.Revision,
		TaskID: task.ID, Phase: entryRemovalTombstonePhase(intent), CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	return task, marker, tombstone, intent
}

func entryDeletionTestProjection(
	t *testing.T,
	store *memoryHierarchyStore,
	entry testkeyvalue.Versioned[testentries.Record],
) testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection] {
	t.Helper()
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID:    entry.Record.EnvironmentID,
		RevisionID:       ids.NewAt(ids.KindTask, serviceRecordTestTime().Add(5*time.Hour), 9150),
		RenderGeneration: 4,
		Entries:          []testentries.Record{testentries.CloneRecord(entry.Record)},
	})
	value, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	result, err := store.Transact(context.Background(), []testkeyvalue.Condition{{
		Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(entry.Record.EnvironmentID),
	}}, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(entry.Record.EnvironmentID), Value: value,
	}})
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Environment projection = %#v/%v", result, err)
	}
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: projection, Revision: result.Revision, ReadRevision: result.Revision,
	}
}

func assertEntryRemovalRetained(
	t *testing.T,
	repository *EntryRepository,
	store *memoryHierarchyStore,
	entry testkeyvalue.Versioned[testentries.Record],
	generationIDs []string,
) {
	t.Helper()
	visible, err := repository.GetEntry(context.Background(), entry.Record.Entry.ID)
	if err != nil || !testentries.EqualRecord(visible.Record, entry.Record) {
		t.Fatalf("GetEntry(retained) = %#v/%v", visible, err)
	}
	assertEntryGenerations(t, store, entry.Record.Entry.ID, generationIDs, true)
}

func assertEntryGenerations(
	t *testing.T,
	store *memoryHierarchyStore,
	entryID string,
	generationIDs []string,
	want bool,
) {
	t.Helper()
	for _, generationID := range generationIDs {
		stored, err := store.Get(context.Background(), testentryvalues.PlainKey(entryID, generationID))
		if err != nil || (stored.Entry != nil) != want {
			t.Fatalf("Get(Entry generation %s) = %#v/%v, want present %t", generationID, stored, err, want)
		}
	}
}

type entryDeletionCloudflareRaceStore struct {
	hierarchyStore
	beforeTransact func() error
	injected       bool
}

func (store *entryDeletionCloudflareRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if store.beforeTransact != nil {
		before := store.beforeTransact
		store.beforeTransact = nil
		store.injected = true
		if err := before(); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
	}
	return store.hierarchyStore.Transact(ctx, conditions, mutations)
}

func assertEntryRemovalPublicationAbsent(
	t *testing.T,
	store *memoryHierarchyStore,
	task TaskRecord,
	marker testidempotency.IdempotencyMarker,
) {
	t.Helper()
	keys := []string{
		testtaskjournal.TaskStorageKey(task.ID),
		testtaskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
		testtaskjournal.TaskActiveOperationKey(task.OperationID),
		testtaskjournal.TaskQueueKey(task.Executor, task.ID),
		testenvironmentchanges.EntryRemovalIntentKey(task.ID),
		testdeletions.TombstoneKey(string(testdeletions.DeletionTargetEntry), task.Target),
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	keys = append(keys, markerKey)
	if marker.ReplayTarget != nil {
		replayKey, replayErr := testidempotency.IdempotencyReplayTargetKey(
			*marker.ReplayTarget, marker.Locator.Method, marker.Locator.Route, marker.Locator.Key,
		)
		if replayErr != nil {
			t.Fatalf("idempotencyReplayTargetKey() error = %v", replayErr)
		}
		keys = append(keys, replayKey)
	}
	for _, key := range keys {
		if value := mustOptionalKey(t, store, key); value != nil {
			t.Fatalf("failed Entry deletion published %s = %#v", key, value)
		}
	}
}

func assertEntryRetryPublicationAbsent(
	t *testing.T,
	store *memoryHierarchyStore,
	source TaskRecord,
	retryID string,
	marker testidempotency.IdempotencyMarker,
) {
	t.Helper()
	keys := []string{
		testtaskjournal.TaskStorageKey(retryID),
		testtaskjournal.TaskOperationIndexKey(source.OperationID, retryID),
		testtaskjournal.TaskActiveOperationKey(source.OperationID),
		testtaskjournal.TaskQueueKey(source.Executor, retryID),
		testenvironmentchanges.EntryRemovalIntentKey(retryID),
		testdeletions.TombstoneKey(string(testdeletions.DeletionTargetEntry), source.Target),
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey(retry) error = %v", err)
	}
	keys = append(keys, markerKey)
	if marker.ReplayTarget != nil {
		replayKey, replayErr := testidempotency.IdempotencyReplayTargetKey(
			*marker.ReplayTarget, marker.Locator.Method, marker.Locator.Route, marker.Locator.Key,
		)
		if replayErr != nil {
			t.Fatalf("idempotencyReplayTargetKey(retry) error = %v", replayErr)
		}
		keys = append(keys, replayKey)
	}
	for _, key := range keys {
		if value := mustOptionalKey(t, store, key); value != nil {
			t.Fatalf("failed Entry retry published %s = %#v", key, value)
		}
	}
}
