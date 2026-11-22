package softwareactivation

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/consolerelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	activation "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
)

func TestConsoleParentCompletionUsesAttemptReceiptAfterLiveActivationChanges(t *testing.T) {
	api := []byte(`{"openapi":"3.1.0"}`)
	desired := consoleTestArtifact("console/v0.0.2", "a", consolerelease.APIHash(api))
	prior := consoleTestArtifact("console/v0.0.1", "b", consolerelease.APIHash(api))
	succeeded := newConsoleParentFixture(t, desired, desired)
	failed := newConsoleParentFixture(t, desired, prior)

	// Both attempts captured their terminal observation before C became the
	// Console Store's live release.
	live, err := consolerelease.New(filepath.Join(t.TempDir(), "console"), fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte("current-c")},
	}, "console/v0.0.3")
	if err != nil {
		t.Fatal(err)
	}
	if err := live.BindAPIDocument(api); err != nil {
		t.Fatal(err)
	}
	liveCurrent, found := live.Current()
	if !found {
		t.Fatal("live Console C is unavailable")
	}

	if liveCurrent == desired || liveCurrent == prior {
		t.Fatal("live Console fixture did not move to C")
	}
	for _, test := range []struct {
		name       string
		fixture    consoleParentCompletionFixture
		outcome    workexecution.Outcome
		wasApplied bool
	}{
		{name: "recorded B B finishes after C", fixture: succeeded,
			outcome: workexecution.Succeeded, wasApplied: true},
		{name: "recorded B A failure finishes after C", fixture: failed, outcome: workexecution.Failed},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := consoleParentService(t, test.fixture, live, test.fixture.receipt)
			_, _, _, applied, _, err := service.finishedItem(t.Context(), test.fixture.view, test.fixture.record)
			if err != nil {
				t.Fatal(err)
			}
			if test.fixture.attempt.Attempt.Outcome != test.outcome || applied != test.wasApplied {
				t.Fatalf("outcome=%q applied=%v; want %q, %v", test.fixture.attempt.Attempt.Outcome,
					applied, test.outcome, test.wasApplied)
			}
		})
	}

	fixture := succeeded
	unfinished := fixture.attempt.Attempt
	unfinished.FinishedAt, unfinished.Result, unfinished.Outcome = nil, nil, ""
	forgedCurrent := consoleTestArtifact("console/v0.0.4", "c", consolerelease.APIHash(api))
	forged, _, err := activation.PrepareConsoleReceipt(fixture.record, unfinished, forgedCurrent, true,
		*fixture.attempt.Attempt.FinishedAt)
	if err != nil {
		t.Fatal(err)
	}
	service := consoleParentService(t, fixture, live, forged.Mutations[0].Value)
	if _, _, _, _, _, err := service.finishedItem(t.Context(), fixture.view, fixture.record); err == nil {
		t.Fatal("tampered current artifact was accepted as the terminal attempt evidence")
	}
}

func TestConsoleAttemptRetirementKeepsReceiptUntilHistoryCollection(t *testing.T) {
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	input := workexecution.Input{ID: ids.NewAt(ids.KindPlan, at, 1), SHA256: strings.Repeat("a", 64)}
	evidence := workexecution.Input{ID: ids.NewAt(ids.KindPlan, at, 2), SHA256: strings.Repeat("b", 64)}
	taskID := ids.NewAt(ids.KindTask, at, 1)
	workID := ids.NewAt(ids.KindWorkItem, at, 1)
	attemptID := ids.NewAt(ids.KindAttempt, at, 1)
	started := at.Add(time.Minute)
	finished := at.Add(2 * time.Minute)
	retainUntil := at.Add(3 * time.Minute)
	collectionStarted := at.Add(4 * time.Minute)
	task := workexecution.Task{
		ID: taskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionActivate,
		TargetID: activation.TargetID(), TargetName: "Groundplane software", Intent: input,
		CreatedAt: at, UpdatedAt: collectionStarted, Deadline: at.Add(time.Hour),
		ExpectedItems: 1, PublicationComplete: true, Progress: workexecution.Progress{Succeeded: 1},
		Outcome: workexecution.Succeeded, FinishedAt: &finished, RetainUntil: &retainUntil,
		CollectionStartedAt: &collectionStarted,
	}
	item := workexecution.Item{
		ID: workID, TaskID: taskID, Operation: workexecution.ConsoleSoftware,
		TargetID: activation.TargetID(), Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims: []workexecution.Claim{{ScopeID: activation.TargetID()}}, Input: &input,
		Phase: workexecution.Settled, AttemptNumber: 1, Outcome: workexecution.Succeeded,
		Result: &evidence, Cleanup: workexecution.CleanupNone,
		CreatedAt: at, UpdatedAt: finished, Deadline: task.Deadline,
	}
	attempt := workexecution.Attempt{
		ID: attemptID, WorkItemID: workID, TaskID: taskID, Number: 1,
		Executor:  workexecution.Identity{Executor: workexecution.Controller, Generation: 1},
		Direction: workexecution.Forward, Input: input, AssignedAt: at.Add(30 * time.Second),
		Deadline: task.Deadline, StartedAt: &started, FinishedAt: &finished,
		Outcome: workexecution.Succeeded, Result: &evidence,
	}
	view := workqueue.View{
		Task: workqueue.TaskSnapshot{Task: task, Revision: 11},
		Item: workqueue.ItemSnapshot{Item: item, Revision: 12}, ReadRevision: 13,
	}
	snapshot := workqueue.AttemptSnapshot{Attempt: attempt, Revision: 14}
	owner := &ConsoleWork{}

	beforeCollection := view
	beforeCollection.Task.Task.CollectionStartedAt = nil
	if _, err := owner.PrepareAttemptRetirement(beforeCollection, snapshot); err == nil {
		t.Fatal("Console receipt could be retired before task history collection")
	}
	change, err := owner.PrepareAttemptRetirement(view, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	wantAttempt, wantReceipt := false, false
	for _, mutation := range change.Mutations {
		wantAttempt = wantAttempt || mutation.Key == workqueue.AttemptKey(attemptID)
		wantReceipt = wantReceipt || mutation.Key == activation.ConsoleResultKey(attemptID)
	}
	if !wantAttempt || !wantReceipt {
		t.Fatalf("attempt and Console receipt were not retired together: %#v", change.Mutations)
	}
}

type consoleParentCompletionFixture struct {
	record  keyvalue.Versioned[activation.Record]
	view    workqueue.View
	attempt workqueue.AttemptSnapshot
	receipt []byte
}

func newConsoleParentFixture(
	t *testing.T,
	desired consolerelease.Artifact,
	observed consolerelease.Artifact,
) consoleParentCompletionFixture {
	t.Helper()
	at := time.Date(2026, 11, 22, 13, 0, 0, 0, time.UTC)
	digest := desired.BundleSHA256
	source := preparation.ResolvedSource{Selection: preparation.SelectionConsole,
		SourceKind: preparation.Release, OriginalRef: desired.Release, Platform: preparation.HostPlatform()}
	source.ConsoleRelease = &preparation.ResolvedRelease{OriginalRef: desired.Release,
		ResolvedSHA: strings.Repeat("d", 40), CommitTime: at,
		Assets: []preparation.ReleaseAsset{{Name: "groundplane-console.tar.gz", Size: 1, SHA256: digest}}}
	prepared := preparation.Result{Source: source,
		Console: &preparation.ConsoleResult{Artifact: desired}}
	preparationTaskID := ids.NewAt(ids.KindTask, at, 20)
	operationID := ids.NewAt(ids.KindOperation, at, 21)
	preparedRecord := preparationrecord.Record{TaskID: preparationTaskID, OperationID: operationID,
		InputSHA256: strings.Repeat("e", 64), Source: source,
		Progress:  preparation.Progress{Phase: preparation.PhaseVerified, Result: prepared},
		CreatedAt: at, UpdatedAt: at}
	if err := preparationrecord.ValidateRecord(preparedRecord); err != nil {
		t.Fatal(err)
	}
	input := activation.Input{OperationID: operationID, Preparation: preparedRecord}
	_, inputSHA256, err := activation.EncodeInput(input)
	if err != nil {
		t.Fatal(err)
	}
	record, err := activation.NewRecord(ids.NewAt(ids.KindTask, at, 22), inputSHA256, input, at)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := record.ConsoleReference()
	if err != nil {
		t.Fatal(err)
	}
	started := at.Add(time.Minute)
	finished := at.Add(2 * time.Minute)
	attempt := workexecution.Attempt{ID: ids.NewAt(ids.KindAttempt, at, 23),
		WorkItemID: record.ConsoleWorkItemID, TaskID: record.TaskID, Number: 1,
		Executor:  workexecution.Identity{Executor: workexecution.Controller, Generation: 1},
		Direction: workexecution.Forward, Input: reference, AssignedAt: at.Add(30 * time.Second),
		Deadline: at.Add(time.Hour), StartedAt: &started}
	versioned := keyvalue.Versioned[activation.Record]{Record: record, Revision: 5, ReadRevision: 19}
	receipt, completion, err := activation.PrepareConsoleReceipt(versioned, attempt, observed, true, finished)
	if err != nil {
		t.Fatal(err)
	}
	attempt.FinishedAt, attempt.Outcome, attempt.Result = &finished, completion.Outcome, &completion.Evidence
	item := workexecution.Item{ID: record.ConsoleWorkItemID, TaskID: record.TaskID,
		Operation: workexecution.ConsoleSoftware, TargetID: activation.TargetID(),
		Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims:    []workexecution.Claim{{ScopeID: activation.TargetID()}}, Input: &reference,
		Phase: workexecution.Settled, AttemptNumber: 1, Outcome: completion.Outcome,
		Result: &completion.Evidence, Cleanup: workexecution.CleanupNone,
		CreatedAt: at, UpdatedAt: finished, Deadline: at.Add(time.Hour)}
	taskReference, err := record.Reference()
	if err != nil {
		t.Fatal(err)
	}
	progress := workexecution.Progress{Succeeded: 1}
	if completion.Outcome == workexecution.Failed {
		progress = workexecution.Progress{Failed: 1}
	}
	task := workexecution.Task{ID: record.TaskID, Actor: workexecution.OperatorActor,
		Action: workexecution.ActionActivate, TargetID: activation.TargetID(), TargetName: "Groundplane software",
		Intent: taskReference, CreatedAt: at, UpdatedAt: finished, Deadline: at.Add(time.Hour),
		ExpectedItems: 1, PublicationComplete: true, Progress: progress}
	if err := task.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := item.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := attempt.Validate(); err != nil {
		t.Fatal(err)
	}
	return consoleParentCompletionFixture{record: versioned,
		view: workqueue.View{Task: workqueue.TaskSnapshot{Task: task, Revision: 6},
			Item: workqueue.ItemSnapshot{Item: item, Revision: 7}, ReadRevision: 19},
		attempt: workqueue.AttemptSnapshot{Attempt: attempt, Revision: 8},
		receipt: bytes.Clone(receipt.Mutations[0].Value)}
}

func consoleParentService(
	t *testing.T,
	fixture consoleParentCompletionFixture,
	live *consolerelease.Store,
	receipt []byte,
) *Service {
	t.Helper()
	taskRaw, err := workqueue.EncodeTask(fixture.view.Task.Task)
	if err != nil {
		t.Fatal(err)
	}
	itemRaw, err := workqueue.EncodeItem(fixture.view.Item.Item)
	if err != nil {
		t.Fatal(err)
	}
	attemptRaw, err := workqueue.EncodeAttempt(fixture.attempt.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	owner := workqueue.ClaimOwner{TaskID: fixture.view.Task.Task.ID, WorkItemID: fixture.view.Item.Item.ID}
	memberRaw, err := recordcodec.Encode("execution_owner", owner)
	if err != nil {
		t.Fatal(err)
	}
	indexRaw, err := recordcodec.Encode("execution_slot", workqueue.Slot{TaskID: owner.TaskID,
		WorkItemID: owner.WorkItemID, AttemptID: fixture.attempt.Attempt.ID})
	if err != nil {
		t.Fatal(err)
	}
	store := &consoleParentStore{revision: fixture.view.ReadRevision, values: map[string]keyvalue.KeyValue{
		workqueue.TaskKey(owner.TaskID):                                             {Value: taskRaw, ModRevision: fixture.view.Task.Revision},
		workqueue.ItemKey(owner.WorkItemID):                                         {Value: itemRaw, ModRevision: fixture.view.Item.Revision},
		workqueue.TaskItemKey(owner.TaskID, owner.WorkItemID):                       {Value: memberRaw, ModRevision: 9},
		workqueue.AttemptIndexKey(owner.WorkItemID, fixture.attempt.Attempt.Number): {Value: indexRaw, ModRevision: 10},
		workqueue.AttemptKey(fixture.attempt.Attempt.ID):                            {Value: attemptRaw, ModRevision: fixture.attempt.Revision},
		activation.ConsoleResultKey(fixture.attempt.Attempt.ID):                     {Value: bytes.Clone(receipt), ModRevision: 11},
	}}
	for key, value := range store.values {
		value.Key = key
		store.values[key] = value
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	records, err := activation.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	return &Service{queue: queue, records: records, console: live}
}

func consoleTestArtifact(release, fill, apiHash string) consolerelease.Artifact {
	digest := "sha256:" + strings.Repeat(fill, 64)
	return consolerelease.Artifact{Release: release, BundleSHA256: digest,
		ManifestSHA256: digest, APISHA256: apiHash}
}

type consoleParentStore struct {
	revision int64
	values   map[string]keyvalue.KeyValue
}

func (store *consoleParentStore) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	if request.Revision != store.revision {
		return nil, errors.New("unexpected Console parent snapshot")
	}
	values := make([]*keyvalue.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		value, found := store.values[key]
		if found {
			value.Value = bytes.Clone(value.Value)
			values[index] = &value
		}
	}
	return &keyvalue.GetManyResult{Values: values, ReadRevision: store.revision,
		ResponseRevision: store.revision}, nil
}

func (*consoleParentStore) Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error) {
	return nil, errors.New("unexpected Console parent range")
}

func (*consoleParentStore) MeasureTransaction(
	context.Context,
	[]keyvalue.Condition,
	[]keyvalue.Mutation,
) (keyvalue.TransactionBudget, error) {
	return keyvalue.TransactionBudget{}, errors.New("unexpected Console parent transaction measurement")
}

func (*consoleParentStore) Transact(
	context.Context,
	[]keyvalue.Condition,
	[]keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	return keyvalue.TransactionResult{}, errors.New("unexpected Console parent transaction")
}
