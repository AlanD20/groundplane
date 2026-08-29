package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: completed detach replay must prove removed companions stay absent,
// a reused name has an exact successor primary, and corrupt removal evidence
// cannot drive an unbounded companion read.
func TestCompletedAttachDetachReplayRejectsDanglingCompanions(t *testing.T) {
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	attaches, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	agentID := ids.NewAt(ids.KindAgent, testAttachTime, 950)

	grantRecord, grantFacts := testPendingAttach(t, scope, 94, "replay-grant", nil)
	createTestAttach(t, ctx, attaches, scope, grantRecord, &grantFacts)
	if _, found, err := tasks.ClaimNextTask(
		ctx, agentID, 14, grantRecord.CreatedAt.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextTask(grant) found/error = %v/%v", found, err)
	}
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		14,
		grantRecord.TaskID,
		taskAssignmentIDForTest(t, tasks, grantRecord.TaskID),
		TaskStatusCompleted,
		completedComposeTaskResult(),
		grantRecord.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask(grant) error = %v", err)
	}
	grant, err := attaches.GetAttach(ctx, grantRecord.ID)
	if err != nil {
		t.Fatalf("GetAttach(grant) error = %v", err)
	}

	sourceScope := scope
	sourceScope.Grants = []Versioned[AttachRecord]{grant}
	record, facts := testPendingAttach(t, sourceScope, 95, "replay-source", sourceScope.Grants)
	createTestAttach(t, ctx, attaches, sourceScope, record, &facts)
	if _, found, err := tasks.ClaimNextTask(
		ctx,
		agentID,
		15,
		record.CreatedAt.Add(3*time.Second),
	); err != nil ||
		!found {
		t.Fatalf("ClaimNextTask(source) found/error = %v/%v", found, err)
	}
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		15,
		record.TaskID,
		taskAssignmentIDForTest(t, tasks, record.TaskID),
		TaskStatusCompleted,
		completedComposeTaskResult(),
		record.CreatedAt.Add(4*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask(source) error = %v", err)
	}
	ready, err := attaches.GetAttach(ctx, record.ID)
	if err != nil {
		t.Fatalf("GetAttach(source ready) error = %v", err)
	}
	grant, err = attaches.GetAttach(ctx, grantRecord.ID)
	if err != nil {
		t.Fatalf("GetAttach(grant refreshed) error = %v", err)
	}
	sourceScope.Grants = []Versioned[AttachRecord]{grant}

	detachAt := record.CreatedAt.Add(5 * time.Second)
	detachTask := publishTestDetach(t, ctx, attaches, sourceScope, ready, detachAt)
	if _, found, err := tasks.ClaimNextTask(ctx, agentID, 16, detachAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextTask(detach) found/error = %v/%v", found, err)
	}
	assignmentID := taskAssignmentIDForTest(t, tasks, detachTask.ID)
	terminalAt := detachAt.Add(2 * time.Second)
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		16,
		detachTask.ID,
		assignmentID,
		TaskStatusCompleted,
		completedComposeTaskResult(),
		terminalAt,
	); err != nil {
		t.Fatalf("AcknowledgeTask(detach) error = %v", err)
	}

	dependentValue, err := encodeAttachDependentGrantIndex(record.ID, []string{grantRecord.ID})
	if err != nil {
		t.Fatalf("encodeAttachDependentGrantIndex() error = %v", err)
	}
	defer clear(dependentValue)
	unlistedGrantID := ids.NewAt(ids.KindAttach, terminalAt, 961)
	unlistedValue, err := encodeAttachDependentGrantIndex(record.ID, []string{unlistedGrantID})
	if err != nil {
		t.Fatalf("encodeAttachDependentGrantIndex(unlisted) error = %v", err)
	}
	defer clear(unlistedValue)
	companions := map[string]struct {
		key   string
		value []byte
	}{
		"name index": {
			key:   attachNameKey(record.EnvironmentID, record.Name),
			value: []byte(record.ID),
		},
		"owner index": {
			key:   attachOwnerKey(record.EnvironmentID, record.ID),
			value: []byte(record.ID),
		},
		"backing service index": {
			key:   attachBackingServiceKey(record.BackingServiceID, record.ID),
			value: []byte(record.ID),
		},
		"backing project index": {
			key:   attachBackingProjectKey(record.BackingProjectID, record.ID),
			value: []byte(record.ID),
		},
		"service index": {
			key:   attachServiceKey(record.ServiceID, record.ID),
			value: []byte(record.ID),
		},
		"encrypted facts": {key: attachFactsKey(record.ID), value: []byte(record.ID)},
		"grant reverse index": {
			key:   attachGrantedByKey(grantRecord.ID, record.ID),
			value: []byte(record.ID),
		},
		"grant dependent index":          {key: attachDependentGrantKey(record.ID), value: dependentValue},
		"unlisted grant dependent index": {key: attachDependentGrantKey(record.ID), value: unlistedValue},
	}
	for name, companion := range companions {
		if _, err := store.Put(ctx, companion.key, companion.value); err != nil {
			t.Fatalf("Put(%s) error = %v", name, err)
		}
		if _, err := tasks.AcknowledgeTask(
			ctx,
			agentID,
			16,
			detachTask.ID,
			assignmentID,
			TaskStatusCompleted,
			completedComposeTaskResult(),
			terminalAt.Add(time.Second),
		); err == nil {
			t.Fatalf("completed detach replay accepted dangling %s", name)
		}
		if _, err := store.Delete(ctx, companion.key); err != nil {
			t.Fatalf("Delete(%s) error = %v", name, err)
		}
	}
	reusedNameOwnerID := ids.NewAt(ids.KindAttach, terminalAt, 960)
	if _, err := store.Put(
		ctx,
		attachNameKey(record.EnvironmentID, record.Name),
		[]byte(reusedNameOwnerID),
	); err != nil {
		t.Fatalf("Put(reused name owner) error = %v", err)
	}
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		16,
		detachTask.ID,
		assignmentID,
		TaskStatusCompleted,
		completedComposeTaskResult(),
		terminalAt.Add(2*time.Second),
	); !isKind(err, errs.KindInternal) {
		t.Fatalf("completed detach replay with dangling reused name error = %v, want internal", err)
	}
	if _, err := store.Delete(ctx, attachNameKey(record.EnvironmentID, record.Name)); err != nil {
		t.Fatalf("Delete(reused name owner) error = %v", err)
	}
	successorRecord, successorFacts := testPendingAttach(t, scope, 96, record.Name, nil)
	createTestAttach(t, ctx, attaches, scope, successorRecord, &successorFacts)
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		16,
		detachTask.ID,
		assignmentID,
		TaskStatusCompleted,
		completedComposeTaskResult(),
		terminalAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("completed detach replay with exact successor name owner error = %v", err)
	}
	successorKey := attachKey(successorRecord.ID)
	successorValue, err := encodeAttachRecord(successorRecord)
	if err != nil {
		t.Fatalf("encodeAttachRecord(successor) error = %v", err)
	}
	defer clear(successorValue)
	for _, test := range []struct {
		name  string
		value []byte
	}{
		{name: "malformed successor primary", value: []byte("not-an-attach-record")},
		{
			name: "mismatched successor primary",
			value: func() []byte {
				mismatched := successorRecord
				mismatched.Name = "different-name"
				value, encodeErr := encodeAttachRecord(mismatched)
				if encodeErr != nil {
					t.Fatalf("encodeAttachRecord(mismatched successor) error = %v", encodeErr)
				}
				return value
			}(),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer clear(test.value)
			if _, err := store.Put(ctx, successorKey, test.value); err != nil {
				t.Fatalf("Put(%s) error = %v", test.name, err)
			}
			if _, err := tasks.AcknowledgeTask(
				ctx,
				agentID,
				16,
				detachTask.ID,
				assignmentID,
				TaskStatusCompleted,
				completedComposeTaskResult(),
				terminalAt.Add(2*time.Second),
			); !isKind(err, errs.KindInternal) {
				t.Fatalf("completed detach replay with %s error = %v, want internal", test.name, err)
			}
			if _, err := store.Put(ctx, successorKey, successorValue); err != nil {
				t.Fatalf("restore successor primary error = %v", err)
			}
		})
	}
	storedRenderInput, err := attaches.GetAttachTaskRenderInput(ctx, detachTask.PlanID)
	if err != nil {
		t.Fatalf("GetAttachTaskRenderInput() error = %v", err)
	}
	originalRenderInputValue, err := encodeAttachTaskRenderInput(storedRenderInput.Record)
	if err != nil {
		t.Fatalf("encodeAttachTaskRenderInput(original) error = %v", err)
	}
	defer clear(originalRenderInputValue)
	for _, test := range []struct {
		name   string
		mutate func(AttachTaskRenderInput) AttachTaskRenderInput
	}{
		{
			name: "two consumers",
			mutate: func(input AttachTaskRenderInput) AttachTaskRenderInput {
				input.Services = append(input.Services, EnvironmentComposeIdentity{
					ID:   ids.NewAt(ids.KindService, terminalAt.Add(100*time.Second), 999),
					Name: input.Services[0].Name + "-second",
				})
				left, right := input.Services[0].ID, input.Services[1].ID
				if right < left {
					left, right = right, left
				}
				input.ConsumerServiceIDs = []string{left, right}
				return input
			},
		},
		{
			name: "nine grants",
			mutate: func(input AttachTaskRenderInput) AttachTaskRenderInput {
				input.GrantAttachIDs = make([]string, MaximumAttachGrants+1)
				for index := range input.GrantAttachIDs {
					input.GrantAttachIDs[index] = ids.NewAt(
						ids.KindAttach,
						terminalAt.Add(time.Duration(index+1)*time.Second),
						int64(970+index),
					)
				}
				return input
			},
		},
	} {
		t.Run("render evidence "+test.name, func(t *testing.T) {
			corruptValue, encodeErr := encodeAttachTaskRenderInput(test.mutate(storedRenderInput.Record))
			if encodeErr != nil {
				t.Fatalf("encodeAttachTaskRenderInput(%s) error = %v", test.name, encodeErr)
			}
			defer clear(corruptValue)
			renderKey := attachTaskRenderInputKey(detachTask.PlanID)
			if _, err := store.Put(ctx, renderKey, corruptValue); err != nil {
				t.Fatalf("Put(%s render evidence) error = %v", test.name, err)
			}
			guardStore := &attachReplayReadGuardStore{attachTestStore: store, renderKey: renderKey}
			guardTasks, repositoryErr := newTaskRepository(guardStore)
			if repositoryErr != nil {
				t.Fatalf("newTaskRepository(replay guard) error = %v", repositoryErr)
			}
			if _, err := guardTasks.AcknowledgeTask(
				ctx,
				agentID,
				16,
				detachTask.ID,
				assignmentID,
				TaskStatusCompleted,
				completedComposeTaskResult(),
				terminalAt.Add(2*time.Second),
			); !isKind(err, errs.KindInternal) {
				t.Fatalf("completed detach replay with %s error = %v, want internal", test.name, err)
			}
			if !guardStore.sawRenderInput || guardStore.readCompanions {
				t.Fatalf(
					"completed detach replay with %s saw render/companions = %v/%v",
					test.name,
					guardStore.sawRenderInput,
					guardStore.readCompanions,
				)
			}
			if _, err := store.Put(ctx, renderKey, originalRenderInputValue); err != nil {
				t.Fatalf("restore Attach render evidence error = %v", err)
			}
		})
	}
	exclusionKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, record.ID)
	if err != nil {
		t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
	}
	exclusionCases := []struct {
		name     string
		value    []byte
		wantKind errs.Kind
	}{
		{
			name:     "valid",
			value:    testAttachBackupExclusionValue(t, record.EnvironmentID, record.ID, 962),
			wantKind: errs.KindStateConflict,
		},
		{
			name:     "malformed",
			value:    []byte("not-an-exclusion-record"),
			wantKind: errs.KindInternal,
		},
		{
			name: "misbucketed",
			value: testAttachBackupExclusionValue(
				t,
				record.EnvironmentID,
				ids.NewAt(ids.KindAttach, terminalAt, 963),
				964,
			),
			wantKind: errs.KindInternal,
		},
	}
	for _, test := range exclusionCases {
		t.Run("exclusion "+test.name, func(t *testing.T) {
			if _, err := store.Put(ctx, exclusionKey, test.value); err != nil {
				t.Fatalf("Put(%s exclusion) error = %v", test.name, err)
			}
			if _, err := tasks.AcknowledgeTask(
				ctx,
				agentID,
				16,
				detachTask.ID,
				assignmentID,
				TaskStatusCompleted,
				completedComposeTaskResult(),
				terminalAt.Add(2*time.Second),
			); !isKind(err, test.wantKind) {
				t.Fatalf("completed detach replay with %s exclusion error = %v, want %v", test.name, err, test.wantKind)
			}
			if _, err := store.Delete(ctx, exclusionKey); err != nil {
				t.Fatalf("Delete(%s exclusion) error = %v", test.name, err)
			}
		})
	}
	replayTargetKey, err := idempotencyReplayTargetKey(
		IdempotencyReplayTarget{Kind: IdempotencyReplayTargetAttach, ID: record.ID},
		"DELETE",
		"/attaches/{id}",
		detachTask.IdempotencyKey,
	)
	if err != nil {
		t.Fatalf("idempotencyReplayTargetKey() error = %v", err)
	}
	if mustOptionalKey(t, store.memoryHierarchyStore, replayTargetKey) == nil {
		t.Fatal("Attach deletion replay target is missing before corruption test")
	}
	if _, err := store.Delete(ctx, replayTargetKey); err != nil {
		t.Fatalf("Delete(replay target) error = %v", err)
	}
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		16,
		detachTask.ID,
		assignmentID,
		TaskStatusCompleted,
		completedComposeTaskResult(),
		terminalAt.Add(2*time.Second),
	); err == nil {
		t.Fatal("completed detach replay accepted a missing stable replay target")
	}
}

type attachReplayReadGuardStore struct {
	*attachTestStore
	renderKey      string
	sawRenderInput bool
	readCompanions bool
}

func (store *attachReplayReadGuardStore) GetMany(
	ctx context.Context,
	request GetManyRequest,
) (*GetManyResult, error) {
	if store.sawRenderInput && len(request.Keys) > 1 {
		store.readCompanions = true
	}
	if len(request.Keys) == 1 && request.Keys[0] == store.renderKey {
		store.sawRenderInput = true
	}
	return store.attachTestStore.GetMany(ctx, request)
}
