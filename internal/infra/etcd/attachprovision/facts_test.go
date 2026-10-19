package attachprovision

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachoutputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BACK-13: a lost result acknowledgement cannot replace the first encrypted
// facts or their timestamp. Changed output/assignment rejects. This exercises
// the real captured-input/report readers and fact owner over a CAS model;
// the authenticated ResumeView and initial command grant are supplied.
func TestNativeAttachFactsReplayPreservesFirstEncryptedResult(t *testing.T) {
	f := newNativeFactsFixture(t)
	change, err := f.repository.PrepareFacts(t.Context(), f.view, f.input, f.output)
	if err != nil {
		t.Fatal(err)
	}
	f.store.loseReply = true
	if _, err := f.store.Transact(t.Context(), change.Conditions, change.Mutations); !errors.Is(
		err,
		errFactsReplyLost,
	) {
		t.Fatalf("lost reply was not injected after commit: %v", err)
	}
	keyvalue.ZeroMutationBytes(change.Mutations)
	key := blueprintattachoutputs.Key(f.output.AttachID, f.input.Attempt.ID)
	first := bytes.Clone(f.store.values[key].Value)
	f.view.Work.ReadRevision = f.store.revision
	replay := f.output
	replay.Facts = nativeFactsEnvelope(t, f.output.AttachID, "new encryption of same hook output")
	replay.CreatedAt = replay.CreatedAt.Add(time.Second)
	change, err = f.repository.PrepareFacts(t.Context(), f.view, f.input, replay)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.store.Transact(t.Context(), change.Conditions, change.Mutations)
	keyvalue.ZeroMutationBytes(change.Mutations)
	if err != nil || !result.Succeeded || !bytes.Equal(first, f.store.values[key].Value) {
		t.Fatalf("exact replay changed the saved result: committed=%v error=%v", result.Succeeded, err)
	}
	stored, err := blueprintattachoutputs.Decode(f.store.values[key].Value)
	if err != nil || !stored.CreatedAt.Equal(f.output.CreatedAt) ||
		!bytes.Equal(stored.Facts.Ciphertext, f.output.Facts.Ciphertext) {
		t.Fatal("replay replaced the first ciphertext or timestamp")
	}
	for _, variant := range []string{"result", "executor", "input", "attempt"} {
		t.Run(variant, func(t *testing.T) {
			changed := replay
			switch variant {
			case "result":
				changed.ResultSHA256 = strings.Repeat("f", 64)
			case "executor":
				changed.Executor.Generation++
			case "input":
				changed.Input.SHA256 = strings.Repeat("f", 64)
			case "attempt":
				changed.AttemptID = ids.New(ids.KindAttempt)
			}
			f.view.Work.ReadRevision = f.store.revision
			change, err := f.repository.PrepareFacts(t.Context(), f.view, f.input, changed)
			defer keyvalue.ZeroMutationBytes(change.Mutations)
			if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || !bytes.Equal(first, f.store.values[key].Value) {
				t.Fatalf("changed hook result acquired authority: %v", err)
			}
		})
	}
}

// BACK-13: preparation cannot publish over a concurrently replaced attempt or
// capture. No command grant, inactive ownership or replaced executor cannot
// create a first result, even when valid encrypted bytes are supplied.
func TestNativeAttachFactsRequireCommandGrantAndAtomicSourceOwnership(t *testing.T) {
	for _, variant := range []string{"missing grant", "inactive", "replaced executor", "attempt race", "capture race"} {
		t.Run(variant, func(t *testing.T) {
			f := newNativeFactsFixture(t)
			switch variant {
			case "missing grant":
				f.view.Checkpoint = workqueue.CheckpointSnapshot{}
			case "inactive":
				f.view.ActiveAttempt = false
			case "replaced executor":
				f.view.CurrentExecutor = false
			}
			change, err := f.repository.PrepareFacts(t.Context(), f.view, f.input, f.output)
			defer keyvalue.ZeroMutationBytes(change.Mutations)
			if variant == "attempt race" || variant == "capture race" {
				if err != nil {
					t.Fatal(err)
				}
				key := workqueue.AttemptKey(f.input.Attempt.ID)
				if variant == "capture race" {
					key = blueprintattachinputs.Key(f.output.TaskID, f.output.AttachID)
				}
				value := f.store.values[key]
				value.ModRevision++
				f.store.values[key] = value
				result, commitErr := f.store.Transact(t.Context(), change.Conditions, change.Mutations)
				if commitErr != nil || result.Succeeded {
					t.Fatalf("stale ownership committed: %v", commitErr)
				}
			} else if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				t.Fatalf("ungranted facts accepted: %v", err)
			}
			if _, exists := f.store.values[blueprintattachoutputs.Key(f.output.AttachID, f.input.Attempt.ID)]; exists {
				t.Fatal("refused result reached storage")
			}
		})
	}
}

type nativeFactsFixture struct {
	repository *InputRepository
	store      *nativeFactsStore
	view       workqueue.ResumeView
	input      attachexecution.ProvisionInput
	output     blueprintattachoutputs.Generation
}

func newNativeFactsFixture(t *testing.T) nativeFactsFixture {
	t.Helper()
	at := time.Now().UTC().Add(-time.Minute)
	taskID, workID, attachID, operation := ids.New(
		ids.KindTask,
	), ids.New(
		ids.KindWorkItem,
	), ids.New(
		ids.KindAttach,
	), ids.New(
		ids.KindOperation,
	)
	environment, service := ids.New(ids.KindEnvironment), ids.New(ids.KindService)
	definition := &backinghook.Definition{Command: []string{"sh", "-ec", "printf result"}, TimeoutSeconds: 30}
	capture := blueprintattachinputs.Generation{
		ID:                   operation,
		OperationID:          operation,
		ParentTaskID:         taskID,
		RevisionID:           taskID,
		EnvironmentID:        environment,
		AttachID:             attachID,
		AttachName:           "search",
		AuthoredSpecSHA256:   strings.Repeat("a", 64),
		ConsumerServiceID:    service,
		CredentialOwnerID:    attachID,
		BackingProjectID:     ids.New(ids.KindProject),
		BackingEnvironmentID: ids.New(ids.KindEnvironment),
		BackingServiceID:     ids.New(ids.KindService),
		BackingNetworkID:     ids.New(ids.KindNetwork),
		AdapterKey:           "custom",
		WorkItemID:           workID,
		Hook: blueprintattachinputs.HookPlan{
			Attach: definition,
		},
		GeneratedInputs: nativeFactsEnvelope(t, attachID, "captured inputs"),
	}
	reference, err := blueprintattachinputs.WorkInput(capture)
	if err != nil {
		t.Fatal(err)
	}
	task := workexecution.Task{ID: taskID, Actor: workexecution.OperatorActor, Action: workexecution.ActionAttach,
		TargetID: attachID, Intent: reference, ExpectedItems: 1, PublicationComplete: true,
		Progress: workexecution.Progress{Running: 1}, CreatedAt: at, UpdatedAt: at, Deadline: at.Add(time.Hour),
		Scope: workexecution.Scope{
			TenantID:      ids.New(ids.KindTenant),
			ProjectID:     ids.New(ids.KindProject),
			EnvironmentID: environment,
		}}
	identity := workexecution.Identity{
		Executor:   workexecution.Agent,
		HostID:     ids.New(ids.KindHost),
		AgentID:    ids.New(ids.KindAgent),
		Generation: 1,
	}
	attempt := workexecution.Attempt{ID: ids.New(ids.KindAttempt), TaskID: taskID, WorkItemID: workID, Number: 1,
		Executor: identity, Direction: workexecution.Forward, Input: reference, AssignedAt: at, StartedAt: &at, Deadline: task.Deadline}
	item := workexecution.Item{ID: workID, TaskID: taskID, Operation: workexecution.Attach, TargetID: attachID,
		Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: identity.HostID},
		Claims:    []workexecution.Claim{{HostID: identity.HostID, ScopeID: attachID}}, Input: &reference,
		Phase: workexecution.Running, Cleanup: workexecution.CleanupNone, AttemptNumber: 1, ActiveAttemptID: attempt.ID,
		CreatedAt: at, UpdatedAt: at, Deadline: task.Deadline}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	hook := &agentpb.BackingHookProcedure{Event: agentpb.BackingHookEvent_BACKING_HOOK_EVENT_ATTACH,
		Command: append([]string(nil), definition.Command...), TimeoutSeconds: definition.TimeoutSeconds,
		AttachId: attachID, BackingServiceId: capture.BackingServiceID, TenantId: task.Scope.TenantID,
		ProjectId: task.Scope.ProjectID, EnvironmentId: environment, ServiceId: service}
	input := attachexecution.ProvisionInput{Attempt: attempt, Provision: &agentpb.AttachProvisionInput{
		CapturedSource: &agentpb.WorkInputReference{InputId: reference.ID, Sha256: digest}, AttachId: attachID,
		BackingServiceId: capture.BackingServiceID, TenantId: task.Scope.TenantID, ProjectId: task.Scope.ProjectID,
		EnvironmentId: environment, ServiceId: service, Provision: &agentpb.AttachProvisionInput_CustomAttach{CustomAttach: hook}}}
	authority, err := input.Authority()
	if err != nil {
		t.Fatal(err)
	}
	progress, err := attachexecution.NewProvisionProgress(attempt, authority, at)
	if err != nil {
		t.Fatal(err)
	}
	progress, err = progress.Begin(attempt, authority, at)
	if err != nil {
		t.Fatal(err)
	}
	report := attachexecution.ProvisionReport{Progress: progress}
	checkpoint, err := report.CheckpointAuthority(attempt, authority)
	if err != nil {
		t.Fatal(err)
	}
	store := &nativeFactsStore{t: t, revision: 10, values: make(map[string]keyvalue.KeyValue)}
	raw, err := blueprintattachinputs.Encode(capture)
	if err != nil {
		t.Fatal(err)
	}
	store.seed(blueprintattachinputs.Key(taskID, attachID), raw)
	raw, err = attachexecution.EncodeProvisionReport(input, report)
	if err != nil {
		t.Fatal(err)
	}
	store.seed(ReportKey(attempt.ID, 1), raw)
	// These CAS receipt values are supplied by the queue owner, not decoded by
	// this operation. Its actual capture and report are decoded above on reads.
	for _, key := range []string{workqueue.TaskKey(taskID), workqueue.ItemKey(workID), workqueue.AttemptKey(attempt.ID),
		workqueue.CheckpointKey(attempt.ID), workqueue.ExecutorKey(identity)} {
		store.seed(key, []byte("supplied queue receipt"))
	}
	repository, err := NewInputRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	view := workqueue.ResumeView{Work: workqueue.View{Task: workqueue.TaskSnapshot{Task: task, Revision: 1},
		Item: workqueue.ItemSnapshot{Item: item, Revision: 1}, ReadRevision: 10},
		Attempt: workqueue.AttemptSnapshot{Attempt: attempt, Revision: 1},
		Checkpoint: workqueue.CheckpointSnapshot{
			Record:   &checkpoint,
			Revision: 1,
		}, CurrentExecutor: true, ActiveAttempt: true, ExecutorRevision: 1}
	output := blueprintattachoutputs.Generation{EnvironmentID: environment, TaskID: taskID, WorkItemID: workID,
		AttemptID: attempt.ID, Executor: identity, Input: reference, AttachID: attachID, OperationID: operation,
		ResultSHA256: strings.Repeat(
			"b",
			64,
		), Facts: nativeFactsEnvelope(t, attachID, "first saved envelope"), CreatedAt: at.Add(time.Second)}
	return nativeFactsFixture{repository: repository, store: store, view: view, input: input, output: output}
}

func nativeFactsEnvelope(t *testing.T, attachID, ciphertext string) attachments.EncryptedFacts {
	t.Helper()
	value, err := attachments.NewAttachEncryptedFacts(attachID, 1, "age-x25519", "sha256", []byte(ciphertext))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

var errFactsReplyLost = errors.New("injected lost native fact commit response")

type nativeFactsStore struct {
	t         *testing.T
	revision  int64
	values    map[string]keyvalue.KeyValue
	loseReply bool
}

func (store *nativeFactsStore) seed(key string, value []byte) {
	store.values[key] = keyvalue.KeyValue{Key: key, Value: bytes.Clone(value), ModRevision: 1, Version: 1}
}

func (store *nativeFactsStore) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	if request.Revision != store.revision {
		store.t.Fatalf("capture escaped supplied snapshot: %d != %d", request.Revision, store.revision)
	}
	result := &keyvalue.GetManyResult{
		ReadRevision: store.revision,
		Values:       make([]*keyvalue.KeyValue, len(request.Keys)),
	}
	for i, key := range request.Keys {
		if saved, exists := store.values[key]; exists {
			saved.Value = bytes.Clone(saved.Value)
			result.Values[i] = &saved
		}
	}
	return result, nil
}

func (store *nativeFactsStore) Transact(
	_ context.Context,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	for _, condition := range conditions {
		if condition.Prefix {
			store.t.Fatal("unexpected prefix claim")
		}
		if store.values[condition.Key].ModRevision != condition.ModRevision {
			return keyvalue.TransactionResult{}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		if mutation.Type != keyvalue.MutationPut || mutation.Prefix {
			store.t.Fatal("fact publication attempted unrelated deletion")
		}
		previous := store.values[mutation.Key]
		store.values[mutation.Key] = keyvalue.KeyValue{Key: mutation.Key, Value: bytes.Clone(mutation.Value),
			ModRevision: store.revision, Version: previous.Version + 1}
	}
	if store.loseReply {
		store.loseReply = false
		return keyvalue.TransactionResult{}, errFactsReplyLost
	}
	return keyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *nativeFactsStore) Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error) {
	store.t.Fatal("facts attempted unrelated range read")
	return nil, nil
}

func (store *nativeFactsStore) MeasureTransaction(
	context.Context,
	[]keyvalue.Condition,
	[]keyvalue.Mutation,
) (keyvalue.TransactionBudget, error) {
	store.t.Fatal("facts attempted separate publication")
	return keyvalue.TransactionBudget{}, nil
}
