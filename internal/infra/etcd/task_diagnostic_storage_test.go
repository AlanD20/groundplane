package etcd

import (
	"bytes"
	"compress/gzip"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestAttemptArchiveUpdatesOnlyTheChangedAttempt(t *testing.T) {
	// TASK-18: each eviction must not rewrite every prior attempt or the Task
	// primary; unchanged summaries must retain their exact bytes and versions.
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	var first *keyvalue.KeyValue
	for sequence := uint64(1); sequence <= 3; sequence++ {
		input := taskEventInput(task.ID, sequence, taskjournal.TaskEventStatePending)
		if sequence > 1 {
			input.Identity.Attempt = 2
		}
		_, digest, err := taskjournal.CanonicalTaskEventPayload(input.State, input.Payload)
		if err != nil {
			t.Fatal(err)
		}
		event := taskjournal.TaskEventRecord{Identity: input.Identity, Sequence: sequence,
			State: input.State, Payload: input.Payload, PayloadSHA256: digest, ReceivedAt: task.CreatedAt.Add(time.Duration(sequence) * time.Second)}
		current, err := store.Get(t.Context(), taskjournal.TaskStorageKey(task.ID))
		if err != nil {
			t.Fatal(err)
		}
		conditions, mutations, err := repository.prepareTrimmedAttemptHistory(
			t.Context(),
			task,
			event,
			current.ReadRevision,
		)
		if err != nil {
			t.Fatal(err)
		}
		result, err := store.Transact(t.Context(), conditions, mutations)
		if err != nil || !result.Succeeded {
			t.Fatalf("archive update: %+v %v", result, err)
		}
		read, err := store.Get(t.Context(), taskjournal.TaskAttemptSummaryKey(task.ID, 1))
		if err != nil || read.Entry == nil {
			t.Fatalf("first attempt missing: %v", err)
		}
		if first == nil {
			first = read.Entry
		} else if first.ModRevision != read.Entry.ModRevision || !bytes.Equal(first.Value, read.Entry.Value) {
			t.Fatal("unchanged attempt was rewritten")
		}
		primary, err := store.Get(t.Context(), taskjournal.TaskStorageKey(task.ID))
		if err != nil || primary.Entry.ModRevision != current.Entry.ModRevision {
			t.Fatal("archive update rewrote the Task")
		}
	}
	current, err := store.Get(t.Context(), taskjournal.TaskStorageKey(task.ID))
	if err != nil {
		t.Fatal(err)
	}
	history, _, err := repository.readAttemptArchive(t.Context(), task.ID, current.ReadRevision)
	if err != nil {
		t.Fatal(err)
	}
	if history.ThroughSequence != 3 || len(history.Summaries) != 2 || history.Summaries[1].LastSequence != 3 {
		t.Fatalf("lost attempt history: %+v", history)
	}
}

func TestAttemptSummaryRejectsUnboundedAndTrailingCompressedData(t *testing.T) {
	// TASK-18: corrupt diagnostic bytes must not bypass the memory bound or
	// conceal a second record behind an otherwise valid compressed summary.
	input := taskEventInput(validTaskRecord(taskJournalTime()).ID, 1, taskjournal.TaskEventStatePending)
	summary := taskjournal.AttemptSummary{Identity: input.Identity, FirstSequence: 1, LastSequence: 1,
		State: input.State, LastReceivedAt: taskJournalTime()}
	valid, err := taskjournal.EncodeAttemptSummary(summary)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := recordcodec.Encode("task-attempt-summary", summary)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, bytes.Repeat([]byte(" "), taskjournal.MaximumAttemptHistoryBytes)...)
	var bomb bytes.Buffer
	writer := gzip.NewWriter(&bomb)
	if _, err := writer.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		value []byte
	}{
		{"decoded size", bomb.Bytes()},
		{"encoded size", bytes.Repeat([]byte("x"), taskjournal.MaximumAttemptHistoryBytes+1)},
		{"second gzip member", append(append([]byte(nil), valid...), valid...)},
		{"trailing bytes", append(append([]byte(nil), valid...), 'x')},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := taskjournal.DecodeAttemptSummary(test.value); err == nil {
				t.Fatal("unsafe compressed summary accepted")
			}
		})
	}
}

func TestDiagnosticExpiryPreservesImmutableTaskAndResumesAfterInterruption(t *testing.T) {
	// TASK-17: shorter diagnostic retention must not rewrite the terminal
	// receipt source or lose replay authority when a cleanup commit is ambiguous.
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	task := validTaskRecord(taskJournalTime())
	createLifecycleTask(t, repository, task)
	terminal, err := repository.AbortPendingTask(t.Context(), task.ID, task.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.Get(t.Context(), taskjournal.TaskStorageKey(task.ID))
	if err != nil {
		t.Fatal(err)
	}
	at := terminal.Record.FinishedAt.Add(2 * time.Hour)
	store.failAfterCommit(errs.New(errs.KindStorageUnavailable, "lost cleanup response"))
	count, err := repository.ExpireTaskDiagnostics(t.Context(), at, time.Hour, 1)
	if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
		t.Fatalf("lost expiry response: %d %v", count, err)
	}
	after, err := store.Get(t.Context(), taskjournal.TaskStorageKey(task.ID))
	if err != nil || after.Entry.ModRevision != before.Entry.ModRevision ||
		!bytes.Equal(after.Entry.Value, before.Entry.Value) {
		t.Fatal("expiry changed immutable Task bytes")
	}
	repository, err = newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	count, err = repository.ExpireTaskDiagnostics(t.Context(), at.Add(time.Minute), time.Hour, 1)
	if err != nil || count != 0 {
		t.Fatalf("restarted cleanup repeated expiry: %d %v", count, err)
	}
	snapshot, err := repository.ListTaskEvents(t.Context(), task.ID, 0)
	if err != nil || snapshot.DiagnosticsExpiredAt == nil || snapshot.Task.Status != taskjournal.TaskStatusAborted {
		t.Fatalf("retained outcome unavailable: %+v %v", snapshot, err)
	}
}
