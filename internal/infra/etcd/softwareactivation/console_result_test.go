package softwareactivation

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/consolerelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
)

func TestConsoleResultRemainsBoundAfterLaterActivation(t *testing.T) {
	record, _, _ := stagingReceiptFixture(t, preparation.SelectionConsole, 80)
	attempt := consoleAttemptFixture(t, record, 80)
	desired := record.Record.Input.Preparation.Progress.Result.Console.Artifact
	prior := otherConsoleArtifact(desired, "1")
	later := otherConsoleArtifact(desired, "2")

	tests := []struct {
		name    string
		current consolerelease.Artifact
		outcome workexecution.Outcome
	}{
		{name: "successful B B receipt survives C", current: desired, outcome: workexecution.Succeeded},
		{name: "failed B A receipt survives C", current: prior, outcome: workexecution.Failed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observedAt := record.Record.CreatedAt.Add(3 * time.Minute)
			change, accepted, err := PrepareConsoleReceipt(record, attempt, test.current, true, observedAt)
			if err != nil {
				t.Fatal(err)
			}
			settled := finishConsoleAttempt(attempt, accepted, observedAt)
			raw := bytes.Clone(change.Mutations[0].Value)
			repository, err := NewRepository(&consoleReceiptReadStore{key: ConsoleResultKey(attempt.ID), value: raw})
			if err != nil {
				t.Fatal(err)
			}
			saved, found, err := repository.ReadConsoleResult(t.Context(), settled, 9)
			if err != nil || !found {
				t.Fatalf("read Console receipt: found=%v err=%v", found, err)
			}
			// A later activation may make C current, but parent completion consumes
			// only this immutable B/B or B/A observation.
			if later == saved.Record.Current {
				t.Fatal("fixture did not move the live Console selection")
			}
			_, completed, err := PrepareConsoleResult(record, saved, settled)
			if err != nil {
				t.Fatal(err)
			}
			if completed != accepted || completed.Outcome != test.outcome || !completed.EffectsSettled {
				t.Fatalf("completed = %#v, accepted = %#v", completed, accepted)
			}
		})
	}
}

func TestConsoleResultRejectsForgedReceipt(t *testing.T) {
	record, _, _ := stagingReceiptFixture(t, preparation.SelectionConsole, 90)
	attempt := consoleAttemptFixture(t, record, 90)
	desired := record.Record.Input.Preparation.Progress.Result.Console.Artifact
	observedAt := record.Record.CreatedAt.Add(3 * time.Minute)
	missingArtifact := record
	missingArtifact.Record.Input.Preparation.Progress.Result.Console = nil
	if _, _, err := PrepareConsoleReceipt(missingArtifact, attempt, desired, true, observedAt); err == nil {
		t.Fatal("Console receipt accepted activation without a captured artifact")
	}
	change, accepted, err := PrepareConsoleReceipt(record, attempt, desired, true, observedAt)
	if err != nil {
		t.Fatal(err)
	}
	settled := finishConsoleAttempt(attempt, accepted, observedAt)
	repository, err := NewRepository(&consoleReceiptReadStore{key: ConsoleResultKey(attempt.ID),
		value: bytes.Clone(change.Mutations[0].Value)})
	if err != nil {
		t.Fatal(err)
	}
	saved, found, err := repository.ReadConsoleResult(t.Context(), settled, 9)
	if err != nil || !found {
		t.Fatalf("read Console receipt: found=%v err=%v", found, err)
	}
	for name, mutate := range map[string]func(*ConsoleReceipt){
		"owner": func(receipt *ConsoleReceipt) {
			receipt.TaskID = ids.NewAt(ids.KindTask, record.Record.CreatedAt, 900)
		},
		"input": func(receipt *ConsoleReceipt) {
			receipt.Input.SHA256 = strings.Repeat("f", 64)
		},
		"observation time": func(receipt *ConsoleReceipt) {
			receipt.At = receipt.At.Add(time.Second)
		},
	} {
		t.Run(name, func(t *testing.T) {
			forged := saved
			mutate(&forged.Record)
			if _, _, err := PrepareConsoleResult(record, forged, settled); err == nil {
				t.Fatal("forged Console receipt was accepted")
			}
		})
	}
	forged := saved
	forged.Record.Current = otherConsoleArtifact(desired, "3")
	_, changed, err := PrepareConsoleResult(record, forged, settled)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Evidence == accepted.Evidence {
		t.Fatal("forged current artifact retained the accepted evidence")
	}
}

func finishConsoleAttempt(
	attempt workexecution.Attempt,
	completion workexecution.Completion,
	at time.Time,
) workexecution.Attempt {
	attempt.FinishedAt = &at
	attempt.Outcome = completion.Outcome
	attempt.Result = &completion.Evidence
	return attempt
}

func consoleAttemptFixture(t *testing.T, record keyvalue.Versioned[Record], seed int64) workexecution.Attempt {
	t.Helper()
	reference, err := record.Record.ConsoleReference()
	if err != nil {
		t.Fatal(err)
	}
	assigned := record.Record.CreatedAt.Add(time.Minute)
	started := assigned.Add(time.Second)
	attempt := workexecution.Attempt{ID: ids.NewAt(ids.KindAttempt, record.Record.CreatedAt, seed+100),
		WorkItemID: record.Record.ConsoleWorkItemID, TaskID: record.Record.TaskID, Number: 1,
		Executor:  workexecution.Identity{Executor: workexecution.Controller, Generation: 1},
		Direction: workexecution.Forward, Input: reference, AssignedAt: assigned,
		Deadline: assigned.Add(time.Hour), StartedAt: &started}
	if err := attempt.Validate(); err != nil {
		t.Fatal(err)
	}
	return attempt
}

func otherConsoleArtifact(base consolerelease.Artifact, suffix string) consolerelease.Artifact {
	digest := "sha256:" + strings.Repeat(suffix, 64)
	return consolerelease.Artifact{Release: "console/v0.0." + suffix,
		BundleSHA256: digest, ManifestSHA256: digest, APISHA256: base.APISHA256}
}

type consoleReceiptReadStore struct {
	key   string
	value []byte
}

func (store *consoleReceiptReadStore) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	if len(request.Keys) != 1 || request.Keys[0] != store.key || request.Revision != 9 {
		return nil, errors.New("unexpected Console receipt read")
	}
	return &keyvalue.GetManyResult{Values: []*keyvalue.KeyValue{{Key: store.key,
		Value: bytes.Clone(store.value), ModRevision: 7}}, ReadRevision: 9, ResponseRevision: 9}, nil
}

func (*consoleReceiptReadStore) Transact(
	context.Context,
	[]keyvalue.Condition,
	[]keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	return keyvalue.TransactionResult{}, errors.New("unexpected Console receipt write")
}
