package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestReleaseHookRetryRequiresDurableNotStartedState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 9, 2, 18, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		seed func(t *testing.T, record testscriptexecutions.ScriptExecutionRecord) *testscriptexecutions.ScriptExecutionRecord
	}{
		{
			name: "start authorized",
			seed: func(t *testing.T, record testscriptexecutions.ScriptExecutionRecord) *testscriptexecutions.ScriptExecutionRecord {
				input := scriptCheckpointTestInput(record, now.Add(time.Second))
				input.State = testscriptexecutions.ScriptExecutionStartAuthorized
				input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
					Kind:            testscriptexecutions.ScriptCheckpointEvidenceStartAuthorized,
					StartAuthorized: &testscriptexecutions.ScriptStartAuthorizedEvidence{},
				}
				started, err := testscriptexecutions.AdvanceScriptExecutionRecord(record, input)
				if err != nil {
					t.Fatal(err)
				}
				return &started
			},
		},
		{
			name: "unknown execution",
			seed: func(*testing.T, testscriptexecutions.ScriptExecutionRecord) *testscriptexecutions.ScriptExecutionRecord {
				return nil
			},
		},
		{
			name: "lineage mismatch",
			seed: func(_ *testing.T, record testscriptexecutions.ScriptExecutionRecord) *testscriptexecutions.ScriptExecutionRecord {
				record.CurrentTaskID = ids.NewAt(ids.KindTask, now, 98)
				return &record
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := newMemoryTaskStore()
			repository, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			record := scriptCheckpointTestRecord(now)
			source := releaseHookRetryTestTask(record, record.CurrentTaskID, now)
			retry := releaseHookRetryTestTask(
				record,
				ids.NewAt(ids.KindTask, now.Add(2*time.Second), 20),
				now.Add(2*time.Second),
			)
			retry.RetryOf = source.ID
			retry.Params[testreleaserender.TaskReleasePublicationParam] = source.Params[testreleaserender.TaskReleasePublicationParam]
			retry.Params[testblueprints.EnvironmentDesiredRevisionParam] = source.Params[testblueprints.EnvironmentDesiredRevisionParam]
			if seeded := test.seed(t, record); seeded != nil {
				value, encodeErr := testrecordcodec.Encode("script-execution", *seeded)
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				transaction, seedErr := store.Transact(
					ctx,
					nil,
					[]testkeyvalue.Mutation{
						{
							Type:  testkeyvalue.MutationPut,
							Key:   testscriptexecutions.ScriptExecutionKey(record.ID),
							Value: value,
						},
					},
				)
				if seedErr != nil || !transaction.Succeeded {
					t.Fatalf("seed execution = %#v, %v", transaction, seedErr)
				}
			}
			revision, readErr := store.GetMany(
				ctx,
				testkeyvalue.GetManyRequest{Keys: []string{testscriptexecutions.ScriptExecutionKey(record.ID)}},
			)
			if readErr != nil {
				t.Fatal(readErr)
			}
			_, err = repository.prepareReleaseHookExecutionRetryTransfer(ctx, source, retry, revision.ReadRevision)
			if !errors.Is(err, errs.New(errs.KindScriptRetryUnsafe, "")) {
				t.Fatalf("retry error = %v, want script.retry_unsafe", err)
			}
		})
	}
}

func TestReleaseHookRetryTransfersExactNotStartedAuthority(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 9, 2, 18, 30, 0, 0, time.UTC)
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	record := scriptCheckpointTestRecord(now)
	value, err := testrecordcodec.Encode("script-execution", record)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: testscriptexecutions.ScriptExecutionKey(record.ID), Value: value},
		},
	)
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed execution = %#v, %v", seeded, err)
	}
	source := releaseHookRetryTestTask(record, record.CurrentTaskID, now)
	retry := releaseHookRetryTestTask(record, ids.NewAt(ids.KindTask, now.Add(time.Second), 21), now.Add(time.Second))
	retry.RetryOf = source.ID
	retry.Params[testreleaserender.TaskReleasePublicationParam] = source.Params[testreleaserender.TaskReleasePublicationParam]
	retry.Params[testblueprints.EnvironmentDesiredRevisionParam] = source.Params[testblueprints.EnvironmentDesiredRevisionParam]
	change, err := repository.prepareReleaseHookExecutionRetryTransfer(ctx, source, retry, seeded.Revision)
	if err != nil {
		t.Fatalf("prepare transfer error = %v", err)
	}
	defer change.clear()
	transaction, err := store.Transact(ctx, change.conditions, change.mutations)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("apply transfer = %#v, %v", transaction, err)
	}
	read, err := store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{Keys: []string{testscriptexecutions.ScriptExecutionKey(record.ID)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	transferred, err := testrecordcodec.Decode[testscriptexecutions.ScriptExecutionRecord](
		read.Values[0].Value,
		"script-execution",
	)
	if err != nil {
		t.Fatal(err)
	}
	if transferred.CurrentTaskID != retry.ID || transferred.State != testscriptexecutions.ScriptExecutionNotStarted ||
		transferred.StartAuthorized || transferred.OperationID != record.OperationID ||
		transferred.PlanHash != record.PlanHash || transferred.ID != record.ID {
		t.Fatalf("transferred authority = %#v", transferred)
	}
}

func releaseHookRetryTestTask(
	record testscriptexecutions.ScriptExecutionRecord,
	taskID string,
	createdAt time.Time,
) TaskRecord {
	return TaskRecord{
		ID: taskID, OperationID: record.OperationID, Executor: testtaskjournal.TaskExecutorAgent,
		PlanID: ids.NewAt(ids.KindPlan, record.CreatedAt, 22), PlanHash: record.PlanHash,
		Type: testtaskjournal.TaskUpdate, Target: record.EnvironmentID, CreatedAt: createdAt,
		Owner: testtaskjournal.TaskOwner{
			WorkspaceType: testtaskjournal.TaskWorkspaceTenant,
			EnvironmentID: record.EnvironmentID,
		},
		Params: map[string]string{
			testreleaserender.ReleaseHookStepExecutionParam(record.StepID): record.ID,
			testreleaserender.TaskReleasePublicationParam:                  ids.NewULID(),
			testtaskjournal.TaskMaterializationEnvironmentParam:            record.EnvironmentID,
			testblueprints.EnvironmentDesiredRevisionParam:                 record.CurrentTaskID,
		},
		Steps: []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepOperation, ID: record.StepID}},
	}
}
