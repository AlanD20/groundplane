package etcd

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTaskMaterializationWriterCodecIsStrict(t *testing.T) {
	// Rationale: reconnect recovery treats the writer as authorization, so
	// duplicate, unknown, or malformed durable fields must fail closed.
	at := taskJournalTime()
	record := taskMaterializationWriterRecord{
		EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 710),
		TaskID:        ids.NewAt(ids.KindTask, at, 711), RenderGeneration: 3,
	}
	value, err := encodeTaskMaterializationWriter(record)
	if err != nil {
		t.Fatalf("encodeTaskMaterializationWriter() error = %v", err)
	}
	decoded, err := decodeTaskMaterializationWriter(value)
	if err != nil || decoded != record {
		t.Fatalf("decodeTaskMaterializationWriter() = %#v, %v", decoded, err)
	}
	for _, malformed := range [][]byte{
		bytes.Replace(value, []byte(`"schema":1`), []byte(`"schema":1,"schema":1`), 1),
		bytes.Replace(value, []byte(`"schema":1`), []byte(`"schema":1,"extra":true`), 1),
	} {
		if _, err := decodeTaskMaterializationWriter(malformed); !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("decodeTaskMaterializationWriter(malformed) error = %v", err)
		}
	}
}

func TestTaskRepositorySerializesMaterializationWithoutBlockingUnrelatedTasks(t *testing.T) {
	// Rationale: ADR 0020 requires a durable per-Environment writer while a
	// busy Environment must not head-of-line block unrelated Agent work.
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	at := taskJournalTime()
	environmentID := ids.NewAt(ids.KindEnvironment, at, 701)
	first := materializationLifecycleTask(at, environmentID, 1)
	second := materializationLifecycleTask(at.Add(time.Second), environmentID, 2)
	unrelated := validTaskRecord(at.Add(2 * time.Second))
	createLifecycleTask(t, repository, first)
	createLifecycleTask(t, repository, second)
	createLifecycleTask(t, repository, unrelated)
	agentID := ids.NewAt(ids.KindAgent, at, 702)

	firstClaim, found, err := repository.ClaimNextTask(ctx, agentID, 4, at.Add(3*time.Second))
	if err != nil || !found || firstClaim.Task.Record.ID != first.ID {
		t.Fatalf("first ClaimNextTask() = %#v, %v, %v", firstClaim, found, err)
	}
	assertTaskLifecycleValue(t, store, taskMaterializationWriterKey(environmentID), true)
	recovered, err := repository.ListAgentAssignments(ctx, agentID, 4, 4)
	if err != nil || len(recovered) != 1 || recovered[0].Task.Record.ID != first.ID {
		t.Fatalf("ListAgentAssignments() = %#v, %v", recovered, err)
	}

	unrelatedClaim, found, err := repository.ClaimNextTask(ctx, agentID, 4, at.Add(4*time.Second))
	if err != nil || !found || unrelatedClaim.Task.Record.ID != unrelated.ID {
		t.Fatalf("unrelated ClaimNextTask() = %#v, %v, %v", unrelatedClaim, found, err)
	}
	if _, err := repository.AcknowledgeTask(
		ctx, agentID, 4, unrelated.ID, TaskStatusCompleted, completedComposeTaskResult(), at.Add(5*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask(unrelated) error = %v", err)
	}
	if _, err := repository.AcknowledgeTask(
		ctx, agentID, 4, first.ID, TaskStatusCompleted, completedComposeTaskResult(), at.Add(6*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask(first) error = %v", err)
	}
	assertTaskLifecycleValue(t, store, taskMaterializationWriterKey(environmentID), false)

	secondClaim, found, err := repository.ClaimNextTask(ctx, agentID, 4, at.Add(7*time.Second))
	if err != nil || !found || secondClaim.Task.Record.ID != second.ID {
		t.Fatalf("second ClaimNextTask() = %#v, %v, %v", secondClaim, found, err)
	}
	assertTaskLifecycleValue(t, store, taskMaterializationWriterKey(environmentID), true)
}

func TestTaskRepositoryScansPastAFullPageOfBlockedMaterializations(t *testing.T) {
	// Rationale: fixed page size is transport tuning, not a starvation limit;
	// an unrelated task after a full blocked page must remain claimable.
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	at := taskJournalTime().Add(24 * time.Hour)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 720)
	active := materializationLifecycleTask(at, environmentID, 1)
	createLifecycleTask(t, repository, active)
	agentID := ids.NewAt(ids.KindAgent, at, 721)
	claim, found, err := repository.ClaimNextTask(ctx, agentID, 5, at.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != active.ID {
		t.Fatalf("active ClaimNextTask() = %#v, %v, %v", claim, found, err)
	}
	for index := 0; index < taskClaimQueuePageSize+1; index++ {
		queued := materializationLifecycleTask(
			at.Add(time.Duration(index+2)*time.Second), environmentID, int32(index+2),
		)
		createLifecycleTask(t, repository, queued)
	}
	unrelated := validTaskRecord(at.Add(time.Duration(taskClaimQueuePageSize+4) * time.Second))
	createLifecycleTask(t, repository, unrelated)

	next, found, err := repository.ClaimNextTask(
		ctx, agentID, 5, at.Add(time.Duration(taskClaimQueuePageSize+5)*time.Second),
	)
	if err != nil || !found || next.Task.Record.ID != unrelated.ID {
		t.Fatalf("paginated ClaimNextTask() = %#v, %v, %v", next, found, err)
	}
}

func materializationLifecycleTask(at time.Time, environmentID string, generation int32) TaskRecord {
	task := validTaskRecord(at)
	task.Type = TaskUpdate
	task.Target = environmentID
	task.RenderGeneration = generation
	task.Params[TaskMaterializationEnvironmentParam] = environmentID
	return task
}
