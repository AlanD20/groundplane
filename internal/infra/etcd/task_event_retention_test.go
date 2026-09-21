package etcd

import (
	"errors"
	"testing"
	"time"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: TASK-07/SVC-15 must survive journal saturation without losing
// mutation evidence, duplicating an old event, or partially trimming on replay.
func TestTaskEventRollingWindowPreservesRecoveryAndReplay(t *testing.T) {
	ctx := t.Context()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	task.ComponentActionStepIDs = []string{taskJournalStepID()}
	seedTaskRepositoryRunningTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	for ordinal := uint64(1); ordinal <= 1003; ordinal++ {
		state := testtaskjournal.TaskEventStatePending
		if ordinal == 1 {
			state = testtaskjournal.TaskEventStateRunning
		}
		if ordinal == 2 {
			state = testtaskjournal.TaskEventStateCompleted
		}
		if ordinal == 1001 {
			store.conflictNextTransactions(1)
		}
		if ordinal == 1003 {
			store.failAfterCommit(errs.New(errs.KindStorageUnavailable, "lost response"))
		}
		identityOrdinal := ordinal
		if ordinal <= 2 {
			identityOrdinal = 3 - ordinal
		}
		input := taskEventInput(task.ID, identityOrdinal, state)
		appended, err := repository.AppendTaskEvent(
			ctx,
			input,
			taskJournalTime().Add(time.Duration(ordinal)*time.Millisecond),
		)
		if ordinal == 1003 {
			if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
				t.Fatalf("lost response = %v", err)
			}
			repository, err = newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			appended, err = repository.AppendTaskEvent(ctx, input, taskJournalTime().Add(time.Hour))
			if !appended.Duplicate {
				t.Fatal("lost-response replay appended again")
			}
		}
		if err != nil || appended.Sequence != ordinal {
			t.Fatalf("append %d: %+v, %v", ordinal, appended, err)
		}
		if ordinal == 1002 {
			replay, err := repository.AppendTaskEvent(
				ctx,
				taskEventInput(task.ID, 2, testtaskjournal.TaskEventStateRunning),
				taskJournalTime().Add(time.Hour),
			)
			if err != nil || !replay.Duplicate || replay.Sequence != 1 {
				t.Fatalf("out-of-order eviction lowered replay watermark: %+v %v", replay, err)
			}
		}
	}
	snapshot, err := repository.ListTaskEvents(ctx, task.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Events) != 1000 || snapshot.Events[0].Sequence != 4 || snapshot.Events[999].Sequence != 1003 ||
		snapshot.Task.NextEventSequence != 1004 {
		t.Fatal("rolling journal lost its exact sequence/count boundary")
	}
	if len(snapshot.Task.EventCheckpoints) != 1 || !snapshot.Task.EventCheckpoints[0].Running ||
		!snapshot.Task.EventCheckpoints[0].Completed {
		t.Fatal("trimmed mutation evidence was lost")
	}
	for _, prefix := range []string{testtaskjournal.TaskEventScopePrefix(task.ID), testtaskjournal.TaskEventDedupScopePrefix(task.ID)} {
		page, err := store.Range(ctx, testkeyvalue.RangeRequest{Prefix: prefix, Limit: 1001})
		if err != nil || page.More || len(page.Values) != 1000 {
			t.Fatalf("retained records = %v, %v", page, err)
		}
	}
	for _, ordinal := range []uint64{3, 1003} {
		replay, err := repository.AppendTaskEvent(
			ctx,
			taskEventInput(task.ID, ordinal, testtaskjournal.TaskEventStatePending),
			taskJournalTime().Add(time.Hour),
		)
		if err != nil || !replay.Duplicate || replay.Sequence != ordinal {
			t.Fatalf("replay %d: %+v %v", ordinal, replay, err)
		}
	}
	if _, err := repository.AppendTaskEvent(ctx, taskEventInput(task.ID, 1, testtaskjournal.TaskEventStateRunning), taskJournalTime().Add(time.Hour)); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("old replay = %v", err)
	}
	if _, err := repository.AppendTaskEvent(ctx, taskEventInput(task.ID, 3, testtaskjournal.TaskEventStateFailed), taskJournalTime().Add(time.Hour)); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("changed watermark replay = %v", err)
	}
	assignment := testtaskassignments.TaskAssignmentRecord{
		AssignmentID:    taskEventTestAssignmentID,
		AgentID:         taskEventTestAgentID,
		AgentGeneration: 1,
		ExecutionEpoch:  1,
	}
	procedure := &agentpb.CandidateReleaseProcedure{ConfigurationRestoration: &agentpb.ConfigurationRestoration{
		Files: []*agentpb.ConfigurationFileRestoration{{ForwardStepId: taskJournalStepID()}},
	}}
	effect, evidence, err := repository.releaseCandidateMutationEvidenceAtRevision(
		ctx,
		snapshot.Task,
		assignment,
		procedure,
		snapshot.Revision,
	)
	if err != nil || !effect || len(evidence) != 1 || !evidence[0].Running || !evidence[0].Completed {
		t.Fatalf("trimmed file evidence: %+v %v", evidence, err)
	}
	component, err := repository.releaseComponentEffectAtRevision(ctx, snapshot.Task, assignment, snapshot.Revision)
	if err != nil || !component {
		t.Fatalf("trimmed Component evidence: %t %v", component, err)
	}
	if _, err := repository.OpenTaskEventStream(ctx, task.ID, 1); !errors.Is(
		err,
		errs.New(errs.KindCursorExpired, ""),
	) {
		t.Fatalf("expired cursor: %v", err)
	}
	for _, after := range []uint64{0, 1002} {
		last, count := uint64(0), 0
		_, err := emitTaskEventSuffix(
			ctx,
			snapshot,
			after,
			func(event testtaskjournal.TaskEventRecord) error { last = event.Sequence; count++; return nil },
		)
		want := 1
		if after == 0 {
			want = 1000
		}
		if err != nil || last != 1003 || count != want {
			t.Fatalf("suffix %d: count=%d last=%d err=%v", after, count, last, err)
		}
	}
	stream := &TaskEventStream{taskID: task.ID}
	last, err := stream.consumeEvent(
		ctx, testkeyvalue.Event{Type: testkeyvalue.EventDelete, Key: testtaskjournal.TaskEventKey(task.ID, 3)}, 1003,
		func(testtaskjournal.TaskEventRecord) error { t.Fatal("emitted deletion"); return nil },
	)
	if err != nil || last != 1003 {
		t.Fatalf("live trim: %d %v", last, err)
	}
	terminalizeTaskStream(t, store, repository, task.ID)
	closed, err := repository.OpenTaskEventStream(ctx, task.ID, 1002)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := closed.Run(ctx, func(event testtaskjournal.TaskEventRecord) error {
		count++
		if event.Sequence != 1003 {
			t.Fatal("wrong terminal suffix")
		}
		return nil
	}); err != nil || count != 1 {
		t.Fatalf("terminal drain: %d %v", count, err)
	}
}
