//go:build etcd_acceptance

package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestHierarchyDeletionRealEtcdRootAckIsParentLastAndReplaySafe(t *testing.T) {
	ctx := context.Background()
	endpoint := hierarchyDeletionAcceptanceEndpoint(t)
	prefix := hierarchyDeletionAcceptancePrefix(t, "root")
	store := hierarchyDeletionAcceptanceStore(t, ctx, endpoint, prefix)
	now := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 1)
	seedHierarchyDeletionTenant(t, ctx, store, tenantID, "root-tenant")
	journal, _ := NewHierarchyDeletionRepository(store)
	tasks, _ := NewTaskRepository(store)
	begin := hierarchyDeletionAcceptanceBegin(
		now,
		HierarchyDeletionTargetTenant,
		tenantID,
		HierarchyDeletionOperationTenant,
		"1",
	)
	created, err := journal.Begin(ctx, begin)
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	claim, found, err := tasks.ClaimNextControllerTask(ctx, now.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != begin.TaskID {
		t.Fatalf("ClaimNextControllerTask() = %#v/%t/%v", claim, found, err)
	}
	frozen, err := journal.FreezeMembership(ctx, created.Operation)
	if err != nil {
		t.Fatalf("FreezeMembership() error = %v", err)
	}
	planned := []HierarchyDeletionPlannedAction{{
		ID: "act_" + strings.Repeat("1", 32), NodeID: "tenant:root:finalize", Ordinal: 0,
		ParentOperationID: begin.OperationID, ActionKind: HierarchyDeletionTenantFinalize,
		TargetKind: "tenant", TargetID: tenantID, TargetRevision: frozen.RootRevision,
		ProcedureInput: HierarchyDeletionProcedureInput{
			Kind: HierarchyDeletionProcedureController, ControllerFinalizer: &frozen.RootProcedureInput,
		},
	}}
	actions, err := journal.BindActions(ctx, created.Operation, planned)
	if err != nil {
		t.Fatalf("BindActions() error = %v", err)
	}
	operation, err := journal.AppendActions(ctx, created.Operation, actions, 0, true)
	if err != nil {
		t.Fatalf("AppendActions() error = %v", err)
	}
	root, operation, err := journal.ReadyAction(ctx, operation)
	if err != nil || root == nil {
		t.Fatalf("ReadyAction() = %#v, %v", root, err)
	}
	if _, err = journal.PrepareRootFinalization(ctx, operation, *root, now.Add(time.Second)); err != nil {
		t.Fatalf("PrepareRootFinalization() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close(before restart) error = %v", err)
	}
	restarted := hierarchyDeletionAcceptanceStore(t, ctx, endpoint, prefix)
	restartedTasks, _ := NewTaskRepository(restarted)
	terminalAt := now.Add(3 * time.Second)
	terminal, err := restartedTasks.AcknowledgeControllerTask(ctx, begin.TaskID, TaskStatusCompleted, terminalAt)
	if err != nil {
		t.Fatalf("AcknowledgeControllerTask() error = %v", err)
	}
	if _, err := restartedTasks.AcknowledgeControllerTask(ctx, begin.TaskID, TaskStatusCompleted, terminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(replay) error = %v", err)
	}
	if result, err := restarted.Get(ctx, tenantKey(tenantID)); err != nil || result.Entry != nil {
		t.Fatalf("Tenant after root ack = %#v/%v", result, err)
	}
	lockKey := HierarchyDeletionLockKey(string(HierarchyDeletionTargetTenant), tenantID)
	if result, err := restarted.Get(ctx, lockKey); err != nil || result.Entry != nil {
		t.Fatalf("lock after root ack = %#v/%v", result, err)
	}
	operation, err = NewHierarchyDeletionRepositoryForTest(t, restarted).OperationByTask(ctx, begin.TaskID)
	if err != nil || operation.Tombstone.Phase != HierarchyDeletionRetained || operation.Tombstone.Terminal == nil {
		t.Fatalf("retained operation = %#v/%v", operation, err)
	}
	replayKey, err := HierarchyDeletionReplayTargetKey(begin.OperationID)
	if err != nil {
		t.Fatalf("HierarchyDeletionReplayTargetKey() error = %v", err)
	}
	replayValue, err := restarted.Get(ctx, replayKey)
	if err != nil || replayValue.Entry == nil {
		t.Fatalf("retained replay locator = %#v/%v", replayValue, err)
	}
	var replayLocator HierarchyDeletionReplayLocator
	if err := decodeHierarchyDeletionRecord(replayValue.Entry.Value, hierarchyDeletionSmallRecordBytes, &replayLocator); err != nil {
		t.Fatalf("decode retained replay locator = %v", err)
	}
	replayLocator.OperationKind = HierarchyDeletionOperationProject
	corruptReplay, err := encodeHierarchyDeletionRecord(replayLocator, hierarchyDeletionSmallRecordBytes)
	if err != nil {
		t.Fatalf("encode corrupt replay locator = %v", err)
	}
	corruptResult, err := restarted.Transact(ctx,
		[]Condition{{Key: replayKey, ModRevision: replayValue.Entry.ModRevision}},
		[]Mutation{{Type: MutationPut, Key: replayKey, Value: corruptReplay}},
	)
	if err != nil || !corruptResult.Succeeded {
		t.Fatalf("corrupt replay locator write = %#v/%v", corruptResult, err)
	}
	if _, err := NewHierarchyDeletionRepositoryForTest(t, restarted).OperationByTask(ctx, begin.TaskID); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("corrupt hierarchy replay locator read error = %v, want internal", err)
	}
	completionSummaryKey, _ := HierarchyDeletionCompletionSummaryKey(begin.OperationID)
	if result, err := restarted.Get(ctx, completionSummaryKey); err != nil || result.Entry == nil {
		t.Fatalf("completion summary = %#v/%v", result, err)
	}
	markerKey, err := idempotencyMarkerKey(*terminal.Record.idempotencyMarker)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	marker, err := restarted.Get(ctx, markerKey)
	if err != nil || marker.Entry == nil {
		t.Fatalf("retained marker = %#v/%v", marker, err)
	}
	deleted, err := restarted.Transact(ctx,
		[]Condition{{Key: markerKey, ModRevision: marker.Entry.ModRevision}},
		[]Mutation{{Type: MutationDelete, Key: markerKey}},
	)
	if err != nil || !deleted.Succeeded {
		t.Fatalf("expire marker = %#v/%v", deleted, err)
	}
	pruneAt := terminalAt.Add(100 * 24 * time.Hour)
	pruned := false
	for range 128 {
		if _, err := restartedTasks.PruneExpiredTasks(ctx, pruneAt); err != nil {
			t.Fatalf("PruneExpiredTasks() error = %v", err)
		}
		persisted, err := restarted.Get(ctx, taskKey(begin.TaskID))
		if err != nil {
			t.Fatalf("Get(pruned Task) error = %v", err)
		}
		if persisted.Entry == nil {
			pruned = true
			break
		}
	}
	if !pruned {
		t.Fatal("retained hierarchy deletion journal did not finish bounded pruning")
	}
	intentKey, _ := HierarchyDeletionIntentKey(begin.OperationID)
	for _, key := range []string{
		HierarchyDeletionTombstoneKey(string(HierarchyDeletionTargetTenant), tenantID),
		intentKey,
		completionSummaryKey,
	} {
		persisted, err := restarted.Get(ctx, key)
		if err != nil || persisted.Entry != nil {
			t.Fatalf("pruned hierarchy deletion key %s = %#v/%v", key, persisted, err)
		}
	}
}

func TestHierarchyDeletionRealEtcdFailedAttemptConcurrentRetryTransfersOwnership(t *testing.T) {
	ctx := context.Background()
	endpoint := hierarchyDeletionAcceptanceEndpoint(t)
	prefix := hierarchyDeletionAcceptancePrefix(t, "retry")
	store := hierarchyDeletionAcceptanceStore(t, ctx, endpoint, prefix)
	now := time.Date(2026, 8, 26, 10, 30, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 30)
	seedHierarchyDeletionTenant(t, ctx, store, tenantID, "retry-tenant")
	journal, _ := NewHierarchyDeletionRepository(store)
	tasks, _ := NewTaskRepository(store)
	begin := hierarchyDeletionAcceptanceBegin(
		now,
		HierarchyDeletionTargetTenant,
		tenantID,
		HierarchyDeletionOperationTenant,
		"6",
	)
	if _, err := journal.Begin(ctx, begin); err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	claim, found, err := tasks.ClaimNextControllerTask(ctx, now.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != begin.TaskID {
		t.Fatalf("ClaimNextControllerTask() = %#v/%t/%v", claim, found, err)
	}
	failed, err := tasks.AcknowledgeControllerTask(ctx, begin.TaskID, TaskStatusFailed, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("AcknowledgeControllerTask(failed) error = %v", err)
	}
	retryAt := now.Add(3 * time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 31)
	type retryOutcome struct {
		result IdempotencyTransactionResult
		err    error
	}
	results := make(chan retryOutcome, 2)
	for range 2 {
		peerStore := hierarchyDeletionAcceptanceStore(t, ctx, endpoint, prefix)
		peerTasks, _ := NewTaskRepository(peerStore)
		go func(repository *TaskRepository) {
			marker := pendingRetryMarker(failed.Record, retryID, retryAt, "hierarchy-retry-key-0001")
			result, err := repository.RetryTask(ctx, begin.TaskID, retryID, TaskActorOperator, marker)
			results <- retryOutcome{result: result, err: err}
		}(peerTasks)
	}
	applied := 0
	existing := 0
	for range 2 {
		concurrent := <-results
		if concurrent.err != nil {
			t.Fatalf("RetryTask(concurrent) error = %v", concurrent.err)
		}
		outcome, _, conflict, err := concurrent.result.Classify()
		if err != nil || conflict != nil {
			t.Fatalf("RetryTask(concurrent) classification = %v/%v", conflict, err)
		}
		switch outcome {
		case IdempotencyKnownApplied:
			applied++
		case IdempotencyKnownExisting:
			existing++
		default:
			t.Fatalf("RetryTask(concurrent) unexpected outcome = %v", outcome)
		}
	}
	if applied != 1 || existing != 1 {
		t.Fatalf("concurrent retry outcomes applied/existing = %d/%d", applied, existing)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close(before retry restart) error = %v", err)
	}
	restarted := hierarchyDeletionAcceptanceStore(t, ctx, endpoint, prefix)
	restartedTasks, _ := NewTaskRepository(restarted)
	replayMarker := pendingRetryMarker(failed.Record, retryID, retryAt, "hierarchy-retry-key-0001")
	replay, err := restartedTasks.RetryTask(ctx, begin.TaskID, retryID, TaskActorOperator, replayMarker)
	if err != nil {
		t.Fatalf("RetryTask(restart replay) error = %v", err)
	}
	outcome, _, conflict, err := replay.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownExisting {
		t.Fatalf("RetryTask(restart replay) classification = %v/%v/%v", outcome, conflict, err)
	}
	idempotency, err := NewIdempotencyRepository(restarted)
	if err != nil {
		t.Fatalf("NewIdempotencyRepository() error = %v", err)
	}
	locator, revision, found, err := idempotency.ResolveReplayLocatorAtRevision(
		ctx,
		*begin.Marker.ReplayTarget,
		begin.Marker.Locator.Method,
		begin.Marker.Locator.Route,
		begin.Marker.Locator.Key,
	)
	if err != nil || !found || revision <= 0 || locator != begin.Marker.Locator {
		t.Fatalf("replay-after-successor lookup = %#v/%d/%t/%v", locator, revision, found, err)
	}
	evidence, err := idempotency.ReadAtRevision(ctx, locator, revision)
	if err != nil || evidence == nil {
		t.Fatalf("replay-after-successor evidence = %#v/%v", evidence, err)
	}
	retainedMarker, err := evidence.Marker()
	if err != nil || retainedMarker.TaskID != begin.TaskID {
		t.Fatalf("replay-after-successor Task = %#v/%v", retainedMarker, err)
	}
	operation, err := NewHierarchyDeletionRepositoryForTest(t, restarted).OperationByTask(ctx, retryID)
	if err != nil || operation.Tombstone.CurrentTaskID != retryID ||
		operation.Intent.TaskOperationID != begin.TaskOperationID {
		t.Fatalf("retry operation ownership = %#v/%v", operation, err)
	}
	tenant, err := restarted.Get(ctx, tenantKey(tenantID))
	if err != nil || tenant.Entry == nil {
		t.Fatalf("retry Tenant = %#v/%v", tenant, err)
	}
	tenantRecord, err := decodeTenant(tenant.Entry.Value)
	if err != nil || tenantRecord.DeletionTaskID != retryID {
		t.Fatalf("retry Tenant deletion_task_id = %#v/%v", tenantRecord, err)
	}
}

func TestHierarchyDeletionRealEtcdComponentReceiptsResumeAfterRestart(t *testing.T) {
	ctx := context.Background()
	endpoint := hierarchyDeletionAcceptanceEndpoint(t)
	prefix := hierarchyDeletionAcceptancePrefix(t, "components")
	store := hierarchyDeletionAcceptanceStore(t, ctx, endpoint, prefix)
	now := time.Date(2026, 8, 26, 11, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 10)
	projectID := ids.NewAt(ids.KindProject, now, 11)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 12)
	componentIDs := []string{ids.NewAt(ids.KindComponent, now, 13), ids.NewAt(ids.KindComponent, now, 14)}
	seedHierarchyDeletionEnvironment(t, ctx, store, now, tenantID, projectID, environmentID, componentIDs)
	journal, _ := NewHierarchyDeletionRepository(store)
	tasks, _ := NewTaskRepository(store)
	begin := hierarchyDeletionAcceptanceBegin(
		now,
		HierarchyDeletionTargetEnvironment,
		environmentID,
		HierarchyDeletionOperationEnvironment,
		"2",
	)
	created, err := journal.Begin(ctx, begin)
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	claim, found, err := tasks.ClaimNextControllerTask(ctx, now.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != begin.TaskID {
		t.Fatalf("ClaimNextControllerTask() = %#v/%t/%v", claim, found, err)
	}
	frozen, err := journal.FreezeMembership(ctx, created.Operation)
	if err != nil {
		t.Fatalf("FreezeMembership() error = %v", err)
	}
	planned := make([]HierarchyDeletionPlannedAction, 0, 3)
	rootPrerequisites := make([]int64, 0, 2)
	for index, componentID := range componentIDs {
		var node *HierarchyDeletionMembershipNode
		for nodeIndex := range frozen.Nodes {
			if frozen.Nodes[nodeIndex].TargetID == componentID &&
				frozen.Nodes[nodeIndex].ActionKind == HierarchyDeletionComponentRemove {
				node = &frozen.Nodes[nodeIndex]
				break
			}
		}
		if node == nil {
			t.Fatalf("component %s missing from frozen membership", componentID)
		}
		planned = append(planned, HierarchyDeletionPlannedAction{
			ID: "act_" + strings.Repeat(string(rune('3'+index)), 32), NodeID: node.NodeID,
			Ordinal: int64(index), ParentOperationID: begin.OperationID,
			ActionKind: node.ActionKind, TargetKind: node.TargetKind, TargetID: node.TargetID,
			TargetRevision: node.TargetRevision, ProcedureInput: node.ProcedureInput,
		})
		rootPrerequisites = append(rootPrerequisites, int64(index))
	}
	planned = append(planned, HierarchyDeletionPlannedAction{
		ID: "act_" + strings.Repeat("5", 32), NodeID: "environment:root:finalize", Ordinal: 2,
		ParentOperationID: begin.OperationID, ActionKind: HierarchyDeletionEnvironmentFinalize,
		TargetKind: "environment", TargetID: environmentID, TargetRevision: frozen.RootRevision,
		PrerequisiteOrdinals: rootPrerequisites,
		ProcedureInput: HierarchyDeletionProcedureInput{
			Kind: HierarchyDeletionProcedureController, ControllerFinalizer: &frozen.RootProcedureInput,
		},
	})
	actions, err := journal.BindActions(ctx, created.Operation, planned)
	if err != nil {
		t.Fatalf("BindActions() error = %v", err)
	}
	operation, err := journal.AppendActions(ctx, created.Operation, actions, 0, true)
	if err != nil {
		t.Fatalf("AppendActions() error = %v", err)
	}
	agentID := ids.NewAt(ids.KindAgent, now, 20)
	operation = consumeHierarchyDeletionComponent(
		t,
		ctx,
		store,
		journal,
		tasks,
		operation,
		actions[0],
		agentID,
		1,
		now.Add(time.Second),
	)
	firstCompletionKey, _ := HierarchyDeletionCompletionKey(begin.OperationID, 0)
	firstCompletion, err := store.Get(ctx, firstCompletionKey)
	if err != nil || firstCompletion.Entry == nil {
		t.Fatalf("first component completion = %#v/%v", firstCompletion, err)
	}
	firstDigest := hierarchyDeletionBytesDigest(firstCompletion.Entry.Value)
	if err := store.Close(); err != nil {
		t.Fatalf("Close(before component restart) error = %v", err)
	}
	restarted := hierarchyDeletionAcceptanceStore(t, ctx, endpoint, prefix)
	journal, _ = NewHierarchyDeletionRepository(restarted)
	tasks, _ = NewTaskRepository(restarted)
	operation, err = journal.OperationByTask(ctx, begin.TaskID)
	if err != nil {
		t.Fatalf("OperationByTask(restart) error = %v", err)
	}
	ready, selected, err := journal.ReadyAction(ctx, operation)
	if err != nil || ready == nil || ready.Ordinal != 1 {
		t.Fatalf("ReadyAction(second) = %#v/%v", ready, err)
	}
	firstDispatch, err := journal.PublishOrResumeAgentAction(ctx, selected, *ready, now.Add(5*time.Second))
	if err != nil {
		t.Fatalf("PublishOrResumeAgentAction(first) error = %v", err)
	}
	secondDispatch, err := journal.PublishOrResumeAgentAction(ctx, selected, *ready, now.Add(6*time.Second))
	if err != nil || firstDispatch.CurrentTaskID != secondDispatch.CurrentTaskID ||
		firstDispatch.ChildOperationID != secondDispatch.ChildOperationID {
		t.Fatalf("resumed dispatch identities = %#v/%#v/%v", firstDispatch, secondDispatch, err)
	}
	operation = consumePublishedHierarchyDeletionComponent(
		t, ctx, restarted, journal, tasks, selected, *ready, agentID, 2, now.Add(7*time.Second),
	)
	firstCompletion, err = restarted.Get(ctx, firstCompletionKey)
	if err != nil || firstCompletion.Entry == nil ||
		hierarchyDeletionBytesDigest(firstCompletion.Entry.Value) != firstDigest {
		t.Fatalf("first receipt changed across retry = %#v/%v", firstCompletion, err)
	}
	root, operation, err := journal.ReadyAction(ctx, operation)
	if err != nil || root == nil || root.Ordinal != 2 {
		t.Fatalf("ReadyAction(root) = %#v/%v", root, err)
	}
	if _, err := journal.PrepareRootFinalization(ctx, operation, *root, now.Add(10*time.Second)); err != nil {
		t.Fatalf("PrepareRootFinalization() error = %v", err)
	}
	for _, componentID := range componentIDs {
		if primary, err := restarted.Get(ctx, componentKey(componentID)); err != nil || primary.Entry != nil {
			t.Fatalf("component %s survived: %#v/%v", componentID, primary, err)
		}
		if owner, err := restarted.Get(ctx, componentEnvironmentOwnerKey(environmentID, componentID)); err != nil ||
			owner.Entry != nil {
			t.Fatalf("component owner %s survived: %#v/%v", componentID, owner, err)
		}
	}
}

func consumeHierarchyDeletionComponent(
	t *testing.T,
	ctx context.Context,
	store Store,
	journal *HierarchyDeletionRepository,
	tasks *TaskRepository,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
	agentID string,
	generation uint64,
	at time.Time,
) HierarchyDeletionOperation {
	t.Helper()
	ready, selected, err := journal.ReadyAction(ctx, operation)
	if err != nil || ready == nil || ready.Ordinal != action.Ordinal {
		t.Fatalf("ReadyAction(%d) = %#v/%v", action.Ordinal, ready, err)
	}
	if _, err := journal.PublishOrResumeAgentAction(ctx, selected, *ready, at); err != nil {
		t.Fatalf("PublishOrResumeAgentAction(%d) error = %v", action.Ordinal, err)
	}
	return consumePublishedHierarchyDeletionComponent(
		t,
		ctx,
		store,
		journal,
		tasks,
		selected,
		*ready,
		agentID,
		generation,
		at.Add(time.Second),
	)
}

func consumePublishedHierarchyDeletionComponent(
	t *testing.T,
	ctx context.Context,
	store Store,
	journal *HierarchyDeletionRepository,
	tasks *TaskRepository,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
	agentID string,
	generation uint64,
	at time.Time,
) HierarchyDeletionOperation {
	t.Helper()
	claim, found, err := tasks.ClaimNextTask(ctx, agentID, generation, at)
	if err != nil || !found {
		t.Fatalf("ClaimNextTask(%d) = %#v/%t/%v", action.Ordinal, claim, found, err)
	}
	component, err := store.Get(ctx, componentKey(action.TargetID))
	if err != nil || component.Entry == nil {
		t.Fatalf("component effect authority = %#v/%v", component, err)
	}
	record, err := decodeComponentRecord(component.Entry.Value)
	if err != nil {
		t.Fatalf("decodeComponentRecord() error = %v", err)
	}
	ownerKey := componentEnvironmentOwnerKey(record.Desired.OwnerID, record.Desired.ID)
	kindKey := componentEnvironmentKindKey(record.Desired.OwnerID, record.Desired.Kind)
	indexes, err := store.GetMany(ctx, GetManyRequest{Keys: []string{ownerKey, kindKey}})
	if err != nil || indexes.Values[0] == nil || indexes.Values[1] == nil {
		t.Fatalf("component indexes = %#v/%v", indexes, err)
	}
	transaction, err := store.Transact(ctx,
		[]Condition{
			{Key: componentKey(record.Desired.ID), ModRevision: component.Entry.ModRevision},
			{Key: ownerKey, ModRevision: indexes.Values[0].ModRevision},
			{Key: kindKey, ModRevision: indexes.Values[1].ModRevision},
		},
		[]Mutation{
			{Type: MutationDelete, Key: componentKey(record.Desired.ID)},
			{Type: MutationDelete, Key: ownerKey}, {Type: MutationDelete, Key: kindKey},
		},
	)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("component Agent effect = %#v/%v", transaction, err)
	}
	result := TaskResultRecord{Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone}
	if _, err := tasks.AcknowledgeTask(
		ctx, agentID, generation, claim.Task.Record.ID, claim.Assignment.Record.AssignmentID,
		TaskStatusCompleted, result, at.Add(time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeAgentTask(%d) error = %v", action.Ordinal, err)
	}
	proof, err := journal.AgentTerminalProof(ctx, operation, action)
	if err != nil || proof == nil {
		t.Fatalf("AgentTerminalProof(%d) = %#v/%v", action.Ordinal, proof, err)
	}
	updated, err := journal.ConsumeAgentTerminal(ctx, operation, action, *proof, at.Add(2*time.Second))
	if err != nil {
		t.Fatalf("ConsumeAgentTerminal(%d) error = %v", action.Ordinal, err)
	}
	return updated
}

func hierarchyDeletionAcceptanceEndpoint(t *testing.T) string {
	t.Helper()
	endpoint := os.Getenv("GROUNDPLANE_TEST_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("GROUNDPLANE_TEST_ETCD_ENDPOINT is required")
	}
	return endpoint
}

func hierarchyDeletionAcceptancePrefix(t *testing.T, suffix string) string {
	t.Helper()
	return "/groundplane-hierarchy-acceptance/" + suffix + "-" + time.Now().
		UTC().
		Format("20060102T150405.000000000") +
		"/"
}

func hierarchyDeletionAcceptanceStore(t *testing.T, ctx context.Context, endpoint, prefix string) Store {
	t.Helper()
	store, err := New(ctx, []string{endpoint}, prefix)
	if err != nil {
		t.Fatalf("New(real etcd) error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func NewHierarchyDeletionRepositoryForTest(t *testing.T, store Store) *HierarchyDeletionRepository {
	t.Helper()
	repository, err := NewHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatalf("NewHierarchyDeletionRepository() error = %v", err)
	}
	return repository
}

func hierarchyDeletionAcceptanceBegin(
	now time.Time,
	targetKind HierarchyDeletionTargetKind,
	targetID string,
	operationKind HierarchyDeletionOperationKind,
	digestDigit string,
) HierarchyDeletionBegin {
	taskID := ids.NewAt(ids.KindTask, now, 100)
	taskOperationID := ids.NewAt(ids.KindOperation, now, 101)
	key := "hierarchy-acceptance-idempotency-" + digestDigit
	ciphertext := []byte("protected-" + key)
	digest := sha256.Sum256(ciphertext)
	body, _ := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: taskID})
	marker := IdempotencyMarker{
		Kind: IdempotencyMarkerTask, State: IdempotencyMarkerPending,
		Locator: IdempotencyLocator{
			ScopeKind: IdempotencyScopeKind(targetKind), ScopeID: targetID,
			Method: http.MethodDelete, Route: "/" + string(targetKind) + "/{id}", Key: key,
		},
		Intent: ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
		},
		Response: IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: body},
		TaskID:   taskID, CreatedAt: now, UpdatedAt: now,
	}
	var replayKind IdempotencyReplayTargetKind
	switch targetKind {
	case HierarchyDeletionTargetTenant:
		replayKind = IdempotencyReplayTargetTenant
	case HierarchyDeletionTargetProject:
		replayKind = IdempotencyReplayTargetProject
	case HierarchyDeletionTargetEnvironment:
		replayKind = IdempotencyReplayTargetEnvironment
	case HierarchyDeletionTargetBacking:
		replayKind = IdempotencyReplayTargetBacking
	}
	marker.ReplayTarget = &IdempotencyReplayTarget{Kind: replayKind, ID: targetID}
	idempotencyDigest := sha256.Sum256([]byte(key))
	return HierarchyDeletionBegin{
		OperationID: "del_" + strings.Repeat(digestDigit, 32), TaskOperationID: taskOperationID,
		OperationKind: operationKind, TargetKind: targetKind, TargetID: targetID,
		TaskID: taskID, IdempotencyHash: hex.EncodeToString(idempotencyDigest[:]), Marker: marker,
		CreatedAt: now, DeadlineAt: now.Add(hierarchyDeletionAttemptTimeout),
	}
}

func seedHierarchyDeletionTenant(t *testing.T, ctx context.Context, store Store, tenantID, slug string) {
	t.Helper()
	record := TenantRecord{ID: tenantID, Slug: slug, Name: slug}
	value, _ := encodeTenant(record)
	coordination, _ := encodeHierarchyCoordination(HierarchyCoordinationRecord{
		Schema: 1, TargetKind: HierarchyDeletionTargetTenant, TargetID: tenantID, MutationEpoch: 1,
	})
	transaction, err := store.Transact(
		ctx,
		[]Condition{
			{Key: tenantKey(tenantID)},
			{Key: tenantSlugKey(slug)},
			{Key: HierarchyCoordinationKey("tenant", tenantID)},
		},
		[]Mutation{
			{Type: MutationPut, Key: tenantKey(tenantID), Value: value},
			{Type: MutationPut, Key: tenantSlugKey(slug), Value: []byte(tenantID)},
			{Type: MutationPut, Key: HierarchyCoordinationKey("tenant", tenantID), Value: coordination},
		},
	)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed Tenant = %#v/%v", transaction, err)
	}
}

func seedHierarchyDeletionEnvironment(
	t *testing.T,
	ctx context.Context,
	store Store,
	now time.Time,
	tenantID, projectID, environmentID string,
	componentIDs []string,
) {
	t.Helper()
	tenant := TenantRecord{ID: tenantID, Slug: "tenant", Name: "Tenant"}
	project := ProjectRecord{
		ID:       projectID,
		TenantID: tenantID,
		Slug:     "project",
		Name:     "Project",
		Kind:     ProjectKindTenant,
	}
	environment := EnvironmentRecord{
		ID: environmentID, ProjectID: projectID, Name: "production", NetworkPool: "10.50.0.0/24",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, now, 15), CreatedAt: now,
	}
	tenantValue, _ := encodeTenant(tenant)
	projectValue, _ := encodeProject(project)
	environmentValue, _ := encodeEnvironment(environment)
	mutations := []Mutation{
		{Type: MutationPut, Key: tenantKey(tenantID), Value: tenantValue},
		{Type: MutationPut, Key: tenantSlugKey(tenant.Slug), Value: []byte(tenantID)},
		{Type: MutationPut, Key: projectKey(projectID), Value: projectValue},
		{Type: MutationPut, Key: projectSlugKey(project), Value: []byte(projectID)},
		{Type: MutationPut, Key: projectOwnerKey(project), Value: []byte(projectID)},
		{Type: MutationPut, Key: environmentKey(environmentID), Value: environmentValue},
		{Type: MutationPut, Key: environmentNameKey(projectID, environment.Name), Value: []byte(environmentID)},
		{Type: MutationPut, Key: environmentOwnerKey(projectID, environmentID), Value: []byte(environmentID)},
		{
			Type: MutationPut, Key: environmentBlueprintHeadKey(environmentID),
			Value: []byte(`{"schema":1,"record_id":"` + environment.CreateTaskID + `"}`),
		},
	}
	for _, coordination := range []HierarchyCoordinationRecord{
		{Schema: 1, TargetKind: HierarchyDeletionTargetTenant, TargetID: tenantID, MutationEpoch: 1},
		{Schema: 1, TargetKind: HierarchyDeletionTargetProject, TargetID: projectID, MutationEpoch: 1},
		{Schema: 1, TargetKind: HierarchyDeletionTargetEnvironment, TargetID: environmentID, MutationEpoch: 1},
	} {
		value, _ := encodeHierarchyCoordination(coordination)
		mutations = append(
			mutations,
			Mutation{
				Type:  MutationPut,
				Key:   HierarchyCoordinationKey(string(coordination.TargetKind), coordination.TargetID),
				Value: value,
			},
		)
	}
	for index, componentID := range componentIDs {
		kind := core.ComponentKindIngressCaddy
		if index == 1 {
			kind = core.ComponentKindEdgeCloudflare
		}
		record := ComponentRecord{Desired: ComponentDesiredRecord{
			ID: componentID, Owner: core.ComponentOwnerEnvironment, OwnerID: environmentID, Kind: kind, Enabled: true,
		}}
		value, _ := encodeComponentRecord(record)
		mutations = append(
			mutations,
			Mutation{Type: MutationPut, Key: componentKey(componentID), Value: value},
			Mutation{
				Type:  MutationPut,
				Key:   componentEnvironmentOwnerKey(environmentID, componentID),
				Value: []byte(componentID),
			},
			Mutation{
				Type:  MutationPut,
				Key:   componentEnvironmentKindKey(environmentID, kind),
				Value: []byte(componentID),
			},
		)
	}
	transaction, err := store.Transact(ctx, nil, mutations)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("seed Environment = %#v/%v", transaction, err)
	}
}
