package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a never-applied Entry deletion must atomically publish its Task,
// retain all generations until acknowledgement, then delete them with metadata.
func TestEntryRepositoryCompletesNeverAppliedRemovalAtomically(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, current, generationIDs := entryDeletionTestState(t)
	task, marker, tombstone, intent := entryDeletionTestRecords(t, project, environment, current, nil)
	result, err := repository.BeginEntryDeletionWithTask(
		ctx, environment, project, current, nil, nil, tombstone, intent, task, marker,
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
	terminal, err := tasks.AcknowledgeControllerTask(ctx, task.ID, TaskStatusCompleted, terminalAt)
	if err != nil || terminal.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeControllerTask() = %#v/%v", terminal, err)
	}
	if epoch := mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID); epoch != terminal.Revision {
		t.Fatalf("completed Entry deletion epoch = %d, want %d", epoch, terminal.Revision)
	}
	_, err = repository.GetEntry(ctx, current.Record.Entry.ID)
	if !isKind(err, errs.KindEntryNotFound) {
		t.Fatalf("GetEntry(completed removal) error = %v", err)
	}
	for _, key := range []string{
		entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID),
		deletionTombstoneKey(string(DeletionTargetEntry), current.Record.Entry.ID),
	} {
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
	if err != nil || !found || terminalIntent.Record.Status != TaskStatusCompleted ||
		terminalIntent.Record.TerminalAt == nil || !terminalIntent.Record.TerminalAt.Equal(terminalAt) {
		t.Fatalf("GetEntryRemovalIntent(terminal) = %#v/%v/%v", terminalIntent, found, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, task.ID, TaskStatusCompleted, terminalAt); err != nil {
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
		ctx, environment, project, current, nil, nil, tombstone, intent, task, marker,
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
		ctx, task.ID, TaskStatusFailed, task.CreatedAt.Add(2*time.Second),
	)
	if err != nil || failed.Record.Status != TaskStatusFailed {
		t.Fatalf("AcknowledgeControllerTask(failed) = %#v/%v", failed, err)
	}
	if epoch := mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID); epoch != failed.Revision {
		t.Fatalf("failed Entry deletion epoch = %d, want %d", epoch, failed.Revision)
	}
	assertEntryRemovalRetained(t, repository, store, current, generationIDs)
	retryAt := task.CreatedAt.Add(3 * time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 9001)
	retryMarker := pendingRetryMarker(failed.Record, retryID, retryAt, "entry-retry-key-0001")
	result, err := tasks.RetryTask(ctx, task.ID, retryID, TaskActorOperator, retryMarker)
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
	replay, err := tasks.RetryTask(ctx, task.ID, retryID, TaskActorOperator, retryMarker)
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
	if err != nil || !found || retryIntent.Record.Status != TaskStatusAborted || retryIntent.Record.TerminalAt == nil {
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
	result, err := repository.BeginEntryDeletionWithTask(
		ctx, environment, project, current, &projection, nil, tombstone, intent, task, marker,
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
		task.ID, taskAssignmentIDForTest(t, tasks,

			task.ID),

		TaskStatusCompleted,
		TaskResultRecord{Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone},
		terminalAt)

	if err != nil || terminal.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeTask() = %#v/%v", terminal, err)
	}
	if epoch := mustEnvironmentMutationEpochRevision(t, store, environment.Record.ID); epoch != terminal.Revision {
		t.Fatalf("completed applied Entry deletion epoch = %d, want %d", epoch, terminal.Revision)
	}
	_, err = repository.GetEntry(ctx, current.Record.Entry.ID)
	if !isKind(err, errs.KindEntryNotFound) {
		t.Fatalf("GetEntry(completed removal) error = %v", err)
	}
	assertEntryGenerations(t, store, current.Record.Entry.ID, generationIDs, false)
	projectionRead, err := store.Get(ctx, environmentComposeProjectionKey(environment.Record.ID))
	if err != nil || projectionRead.Entry == nil {
		t.Fatalf("Get(promoted projection) = %#v/%v", projectionRead, err)
	}
	promoted, err := decodeEnvironmentComposeProjection(projectionRead.Entry.Value)
	if err != nil || intent.CandidateProjection == nil ||
		!sameEntryRemovalProjection(promoted, *intent.CandidateProjection) {
		t.Fatalf("promoted projection = %#v/%v", promoted, err)
	}
	for _, key := range []string{
		deletionTombstoneKey(string(DeletionTargetEntry), current.Record.Entry.ID),
		componentTaskActiveEnvironmentKey(environment.Record.ID),
		taskMaterializationWriterKey(environment.Record.ID),
	} {
		stored, getErr := store.Get(ctx, key)
		if getErr != nil || stored.Entry != nil {
			t.Fatalf("finalized key %s = %#v/%v", key, stored, getErr)
		}
	}
}

// Rationale: an enabled Cloudflare Tunnel token is a live stable-id reference,
// and even a disabled singleton must remain revision-fenced so a concurrent
// enable or retarget cannot race Entry deletion publication.
func TestEntryDeletionFencesCloudflareTokenReference(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, current, _ := entryDeletionTestState(t)
	components, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	at := serviceRecordTestTime().Add(7 * time.Hour)
	record, err := NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, at, 1), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environment.Record.ID, Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
		Config:            map[string]any{"token_entry_id": current.Record.Entry.ID},
		GeneratedServices: []string{ids.NewAt(ids.KindService, at, 2)},
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(enabled Cloudflare) error = %v", err)
	}
	cloudflare, err := components.CreateEnvironmentComponent(ctx, environment, project, record)
	if err != nil {
		t.Fatalf("CreateEnvironmentComponent() error = %v", err)
	}
	task, marker, tombstone, intent := entryDeletionTestRecords(t, project, environment, current, nil)
	if _, err := repository.BeginEntryDeletionWithTask(
		ctx, environment, project, current, nil, &cloudflare, tombstone, intent, task, marker,
	); !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("BeginEntryDeletionWithTask(enabled token) error = %v", err)
	}

	desired, err := ProjectComponentRecord(cloudflare.Record)
	if err != nil {
		t.Fatalf("ProjectComponentRecord() error = %v", err)
	}
	desired.Enabled = false
	desired.Config = map[string]any{"token_entry_id": ids.NewAt(ids.KindEnvEntry, at, 3)}
	disabled, err := components.ReplaceDesired(ctx, environment, project, cloudflare, desired)
	if err != nil {
		t.Fatalf("ReplaceDesired(disable) error = %v", err)
	}
	desired.Config = map[string]any{"token_entry_id": ids.NewAt(ids.KindEnvEntry, at, 4)}
	if _, err := components.ReplaceDesired(ctx, environment, project, disabled, desired); err != nil {
		t.Fatalf("ReplaceDesired(retarget) error = %v", err)
	}
	result, err := repository.BeginEntryDeletionWithTask(
		ctx, environment, project, current, nil, &disabled, tombstone, intent, task, marker,
	)
	if err != nil {
		t.Fatalf("BeginEntryDeletionWithTask(stale Component) error = %v", err)
	}
	_, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || !isKind(conflict, errs.KindStateConflict) {
		t.Fatalf("stale Component conflict/error = %v/%v", conflict, classifyErr)
	}
}

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
			context.Background(), environment, project, current, nil, nil, tombstone, intent, task, marker,
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
			context.Background(), environment, project, current, nil, nil, tombstone, intent, task, marker,
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
) (*EntryRepository, *memoryHierarchyStore, Versioned[EnvironmentRecord], Versioned[ProjectRecord],
	Versioned[EntryRecord], []string) {
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
	record, err := NewEntryRecord(environment.Record.ID, entry, firstID)
	if err != nil {
		t.Fatalf("NewEntryRecord() error = %v", err)
	}
	first := testPlainGeneration(environment.Record.ID, entryID, firstID, "staging", now)
	created, err := repository.CreateEntry(
		context.Background(), environment, project, record, EntryValueGeneration{Plain: &first},
	)
	if err != nil {
		t.Fatalf("CreateEntry() error = %v", err)
	}
	secondID := ids.NewAt(ids.KindConfig, now.Add(time.Second), 9102)
	entry.Source.Literal = "production"
	second := testPlainGeneration(environment.Record.ID, entryID, secondID, "production", now.Add(time.Second))
	current, err := repository.ReplaceEntry(
		context.Background(), environment, project, created, entry, secondID, EntryValueGeneration{Plain: &second},
	)
	if err != nil {
		t.Fatalf("ReplaceEntry() error = %v", err)
	}
	return repository, store, environment, project, current, []string{firstID, secondID}
}

func entryDeletionTestRecords(
	t *testing.T,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	entry Versioned[EntryRecord],
	projection *Versioned[EnvironmentComposeProjection],
) (TaskRecord, IdempotencyMarker, DeletionTombstoneRecord, EntryRemovalIntent) {
	t.Helper()
	createdAt := serviceRecordTestTime().Add(6 * time.Hour)
	task := validTaskRecord(createdAt)
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.ID = ids.NewAt(ids.KindTask, createdAt, 9200)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, 9201)
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, 9202)
	task.Executor = TaskExecutorController
	task.Type = TaskRemove
	task.Target = entry.Record.Entry.ID
	task.Params = map[string]string{
		TaskResourceKindParam: TaskResourceEntry, TaskEntryEnvironmentParam: entry.Record.EnvironmentID,
	}
	task.TimeoutSeconds = 30
	task.IdempotencyKey = "entry-remove-key-0001"
	if projection != nil {
		task.Executor = TaskExecutorAgent
		task.TimeoutSeconds = 120
		task.RenderGeneration = int32(projection.Record.RenderGeneration + 1)
		task.Params = map[string]string{
			TaskEntryEnvironmentParam:           entry.Record.EnvironmentID,
			TaskMaterializationEnvironmentParam: entry.Record.EnvironmentID,
			EnvironmentBlueprintRevisionParam:   projection.Record.BlueprintRevisionID,
			TaskComposeArtifactParam: ids.NewAt(
				ids.KindConfig, createdAt, 9203,
			),
		}
	}
	marker := pendingTaskMarker(task)
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: entry.Record.EnvironmentID,
		Method: http.MethodDelete, Route: "/entries/{id}", Key: task.IdempotencyKey,
	}
	replayTarget := IdempotencyReplayTarget{Kind: IdempotencyReplayTargetEntry, ID: entry.Record.Entry.ID}
	marker.ReplayTarget = &replayTarget
	intent, err := NewEntryRemovalIntent(
		task.ID, entry.Record.EnvironmentID, entry.Record.Entry.ID, entry.Revision, projection, createdAt,
	)
	if err != nil {
		t.Fatalf("NewEntryRemovalIntent() error = %v", err)
	}
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetEntry, TargetID: entry.Record.Entry.ID, TargetRevision: entry.Revision,
		TaskID: task.ID, Phase: entryRemovalTombstonePhase(intent), CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	return task, marker, tombstone, intent
}

func entryDeletionTestProjection(
	t *testing.T,
	store *memoryHierarchyStore,
	entry Versioned[EntryRecord],
) Versioned[EnvironmentComposeProjection] {
	t.Helper()
	projection := EnvironmentComposeProjection{
		EnvironmentID:       entry.Record.EnvironmentID,
		BlueprintRevisionID: ids.NewAt(ids.KindTask, serviceRecordTestTime().Add(5*time.Hour), 9150),
		RenderGeneration:    4,
		Entries:             []EntryRecord{cloneEntryRecord(entry.Record)},
	}
	value, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	result, err := store.Transact(context.Background(), []Condition{{
		Key: environmentComposeProjectionKey(entry.Record.EnvironmentID),
	}}, []Mutation{{
		Type: MutationPut, Key: environmentComposeProjectionKey(entry.Record.EnvironmentID), Value: value,
	}})
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Environment projection = %#v/%v", result, err)
	}
	return Versioned[EnvironmentComposeProjection]{
		Record: projection, Revision: result.Revision, ReadRevision: result.Revision,
	}
}

func assertEntryRemovalRetained(
	t *testing.T,
	repository *EntryRepository,
	store *memoryHierarchyStore,
	entry Versioned[EntryRecord],
	generationIDs []string,
) {
	t.Helper()
	visible, err := repository.GetEntry(context.Background(), entry.Record.Entry.ID)
	if err != nil || !equalEntryRecord(visible.Record, entry.Record) {
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
		stored, err := store.Get(context.Background(), plainEntryValueGenerationKey(entryID, generationID))
		if err != nil || (stored.Entry != nil) != want {
			t.Fatalf("Get(Entry generation %s) = %#v/%v, want present %t", generationID, stored, err, want)
		}
	}
}
