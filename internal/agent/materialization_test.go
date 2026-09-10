package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	materializationTaskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationOperationID   = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationPlanID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationStepID        = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationSecondStepID  = "step_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	materializationArtifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationID            = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	materializationSecondID      = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	materializationEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationVolumeDir     = "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
		"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func TestMaterializationInboxAcceptsExactOrderedTransfer(t *testing.T) {
	// Rationale: plaintext may enter an Agent task only through a transfer whose
	// outer correlation and repeated header exactly match its sealed plan step.
	assignment := materializationAssignment(t, []byte("services: {}\n"))
	inbox := newMaterializationInbox()
	if err := inbox.Register(assignment); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	for _, transfer := range materializationTransfers(assignment, []byte("services: {}\n"), 5) {
		if err := inbox.Accept(context.Background(), transfer); err != nil {
			t.Fatalf("Accept() error = %v", err)
		}
	}
	payload, err := inbox.Take(context.Background(), materializationTaskID, materializationStepID)
	if err != nil {
		t.Fatalf("Take() error = %v", err)
	}
	content, err := io.ReadAll(payload.Source)
	if err != nil || !bytes.Equal(content, []byte("services: {}\n")) {
		t.Fatalf("payload content/error = %q/%v", content, err)
	}
	source := payload.Source.(*ownedMaterializationSource)
	if err := source.Close(); err != nil {
		t.Fatalf("Source.Close() error = %v", err)
	}
	if source.content != nil {
		t.Fatal("Source.Close() retained owned plaintext")
	}
}

func TestMaterializationInboxRejectsMalformedTransfer(t *testing.T) {
	// Rationale: duplicate headers, out-of-order chunks, and digest mismatches
	// are protocol violations and must clear rather than publish partial bytes.
	tests := []struct {
		name   string
		mutate func([]*agentpb.MaterializationTransfer)
	}{
		{name: "duplicate header", mutate: func(records []*agentpb.MaterializationTransfer) {
			records[1] = records[0]
		}},
		{name: "out of order", mutate: func(records []*agentpb.MaterializationTransfer) {
			records[1].GetChunk().Sequence = 2
		}},
		{name: "digest mismatch", mutate: func(records []*agentpb.MaterializationTransfer) {
			records[len(records)-1].GetEnd().ChunkCount = 1
			records[1].GetChunk().Content[0] ^= 0xff
		}},
		{name: "wrong plan", mutate: func(records []*agentpb.MaterializationTransfer) {
			records[0].PlanHash[0] ^= 0xff
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assignment := materializationAssignment(t, []byte("services: {}\n"))
			inbox := newMaterializationInbox()
			if err := inbox.Register(assignment); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			records := materializationTransfers(assignment, []byte("services: {}\n"), 64)
			test.mutate(records)
			var rejected bool
			for _, record := range records {
				if err := inbox.Accept(context.Background(), record); err != nil {
					rejected = true
					break
				}
			}
			if !rejected {
				t.Fatal("Accept() accepted malformed transfer")
			}
		})
	}
}

func TestMaterializationRuntimeStreamsVerifiedHelperFrame(t *testing.T) {
	// Rationale: the Agent must translate transient channel records into the
	// hardened helper frame without putting plaintext into a plan or subprocess argument.
	content := []byte("services: {}\n")
	assignment := materializationAssignment(t, content)
	inbox := newMaterializationInbox()
	if err := inbox.Register(assignment); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	for _, transfer := range materializationTransfers(assignment, content, 4) {
		if err := inbox.Accept(context.Background(), transfer); err != nil {
			t.Fatalf("Accept() error = %v", err)
		}
	}
	helper := &decodingMaterializationHelper{}
	runtime, err := NewMaterializationRuntime(helper, nil)
	if err != nil {
		t.Fatalf("NewMaterializationRuntime() error = %v", err)
	}
	step := assignment.Plan.Steps[0]
	payload, err := inbox.Take(context.Background(), assignment.TaskID, step.StepId)
	if err != nil {
		t.Fatalf("Take() error = %v", err)
	}
	if err := runtime.executeStep(context.Background(), assignment, step, payload); err != nil {
		t.Fatalf("executeStep() error = %v", err)
	}
	if helper.volumeDir != materializationVolumeDir || !bytes.Equal(helper.content, content) ||
		helper.header.TaskID() != assignment.TaskID || helper.header.StepID() != step.StepId {
		t.Fatalf("helper request = dir %q, header %#v, content %q", helper.volumeDir, helper.header, helper.content)
	}
}

func TestWorkerExecutesMaterializationAfterVerifiedTransfer(t *testing.T) {
	// Rationale: the worker must reserve the task before accepting bytes, block
	// only that task's step, and resume through the ordinary result path after verification.
	content := []byte("services: {}\n")
	assignment := materializationAssignment(t, content)
	helper := &decodingMaterializationHelper{}
	runtime, err := NewMaterializationRuntime(helper, nil)
	if err != nil {
		t.Fatalf("NewMaterializationRuntime() error = %v", err)
	}
	pool := NewWorkerPoolWithRuntimes(
		1,
		"/var/lib/groundplane/vol",
		nil,
		testLogger(),
		nil,
		nil,
		runtime,
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()
	if err := pool.Submit(ctx, assignment); err != nil {
		cancel()
		<-done
		t.Fatalf("Submit() error = %v", err)
	}
	for _, transfer := range materializationTransfers(assignment, content, 4) {
		if err := pool.AcceptMaterializationTransfer(ctx, transfer); err != nil {
			cancel()
			<-done
			t.Fatalf("AcceptMaterializationTransfer() error = %v", err)
		}
	}
	result := nextWorkerResult(t, pool)
	if result.Terminal != TaskTerminalCompleted || result.Compose == nil || !bytes.Equal(helper.content, content) {
		t.Fatalf("worker result/helper content = %#v/%q", result, helper.content)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WorkerPool.Run() did not stop")
	}
}

func TestWorkerRetiresAndDrainsUnreachedMaterializationAfterTerminalResult(t *testing.T) {
	// Rationale: the Controller may already have queued a later private transfer
	// when an earlier materialization terminalizes the worker. The Agent must
	// clear that late plaintext while retaining exact framing authority through End.
	firstContent := []byte("first materialization\n")
	lateContent := []byte("late private materialization\n")
	assignment := twoMaterializationAssignment(t, firstContent, lateContent)
	helper := &decodingMaterializationHelper{err: errors.New("stop after first materialization")}
	runtime, err := NewMaterializationRuntime(helper, nil)
	if err != nil {
		t.Fatalf("NewMaterializationRuntime() error = %v", err)
	}
	pool := NewWorkerPoolWithRuntimes(
		1,
		"/var/lib/groundplane/vol",
		nil,
		testLogger(),
		nil,
		nil,
		runtime,
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()
	if err := pool.Submit(ctx, assignment); err != nil {
		cancel()
		<-done
		t.Fatalf("Submit() error = %v", err)
	}
	for _, transfer := range materializationTransfersForStep(assignment, 0, firstContent, 7) {
		if err := pool.AcceptMaterializationTransfer(ctx, transfer); err != nil {
			cancel()
			<-done
			t.Fatalf("AcceptMaterializationTransfer(first) error = %v", err)
		}
	}
	result := nextWorkerResult(t, pool)
	if result.Terminal != TaskTerminalFailed || !bytes.Equal(helper.content, firstContent) {
		cancel()
		<-done
		t.Fatalf("worker result/helper content = %#v/%q", result, helper.content)
	}
	assertRetiredMaterializationStepCleared(t, pool.materializations, assignment.TaskID, materializationSecondStepID)

	lateTransfers := materializationTransfersForStep(assignment, 1, lateContent, 5)
	for index, transfer := range lateTransfers {
		if err := pool.AcceptMaterializationTransfer(ctx, transfer); err != nil {
			cancel()
			<-done
			t.Fatalf("AcceptMaterializationTransfer(late record %d) error = %v", index, err)
		}
		if transfer.GetChunk() != nil && len(transfer.GetChunk().GetContent()) != 0 {
			cancel()
			<-done
			t.Fatalf("late record %d retained inbound plaintext", index)
		}
		if transfer.GetEnd() == nil {
			assertRetiredMaterializationStepCleared(
				t,
				pool.materializations,
				assignment.TaskID,
				materializationSecondStepID,
			)
		}
	}
	pool.materializations.mu.Lock()
	_, retained := pool.materializations.tasks[assignment.TaskID]
	pool.materializations.mu.Unlock()
	if retained {
		cancel()
		<-done
		t.Fatal("materialization retirement retained task after valid End")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WorkerPool.Run() did not stop")
	}
}

func TestMaterializationInboxRetiredTransferRejectsMalformedRecords(t *testing.T) {
	// Rationale: retirement discards plaintext, not the correlation, ordering,
	// length, or digest authority required to reject corrupt late records.
	tests := []struct {
		name   string
		mutate func([]*agentpb.MaterializationTransfer)
	}{
		{name: "wrong correlation", mutate: func(records []*agentpb.MaterializationTransfer) {
			records[0].AssignmentId = "assignment_wrong"
		}},
		{name: "wrong sequence", mutate: func(records []*agentpb.MaterializationTransfer) {
			records[1].GetChunk().Sequence++
		}},
		{name: "wrong length", mutate: func(records []*agentpb.MaterializationTransfer) {
			records[0].GetHeader().Length++
		}},
		{name: "wrong digest", mutate: func(records []*agentpb.MaterializationTransfer) {
			records[1].GetChunk().Content[0] ^= 0xff
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			content := []byte("late private materialization\n")
			assignment := materializationAssignment(t, content)
			inbox := newMaterializationInbox()
			if err := inbox.Register(assignment); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			inbox.Retire(assignment.TaskID)
			records := materializationTransfers(assignment, content, len(content))
			test.mutate(records)
			var rejected bool
			for _, record := range records {
				if err := inbox.Accept(context.Background(), record); err != nil {
					rejected = true
					break
				}
			}
			if !rejected {
				t.Fatal("Accept() accepted malformed retired transfer")
			}
		})
	}
}

func TestMaterializationInboxClearsEveryRejectedChunk(t *testing.T) {
	// Rationale: a rejected protobuf record remains transient plaintext even
	// when correlation or lifecycle validation fails before chunk processing.
	content := []byte("rejected private materialization\n")
	tests := []struct {
		name  string
		setup func(*testing.T, *materializationInbox, Assignment) (*agentpb.MaterializationTransfer, context.Context)
	}{
		{name: "canceled context", setup: func(
			t *testing.T,
			_ *materializationInbox,
			assignment Assignment,
		) (*agentpb.MaterializationTransfer, context.Context) {
			t.Helper()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return materializationTransfers(assignment, content, len(content))[1], ctx
		}},
		{name: "wrong correlation", setup: func(
			t *testing.T,
			_ *materializationInbox,
			assignment Assignment,
		) (*agentpb.MaterializationTransfer, context.Context) {
			t.Helper()
			transfer := materializationTransfers(assignment, content, len(content))[1]
			transfer.AssignmentId = "assignment_wrong"
			return transfer, context.Background()
		}},
		{name: "unknown step", setup: func(
			t *testing.T,
			_ *materializationInbox,
			assignment Assignment,
		) (*agentpb.MaterializationTransfer, context.Context) {
			t.Helper()
			transfer := materializationTransfers(assignment, content, len(content))[1]
			transfer.StepId = materializationSecondStepID
			return transfer, context.Background()
		}},
		{name: "extra after end", setup: func(
			t *testing.T,
			inbox *materializationInbox,
			assignment Assignment,
		) (*agentpb.MaterializationTransfer, context.Context) {
			t.Helper()
			for _, transfer := range materializationTransfers(assignment, content, len(content)) {
				if err := inbox.Accept(context.Background(), transfer); err != nil {
					t.Fatalf("Accept(valid) error = %v", err)
				}
			}
			return materializationTransfers(assignment, content, len(content))[1], context.Background()
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assignment := materializationAssignment(t, content)
			inbox := newMaterializationInbox()
			if err := inbox.Register(assignment); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			transfer, ctx := test.setup(t, inbox, assignment)
			if err := inbox.Accept(ctx, transfer); err == nil {
				t.Fatal("Accept() accepted rejected chunk")
			}
			if len(transfer.GetChunk().GetContent()) != 0 {
				t.Fatal("Accept() retained rejected chunk plaintext")
			}
		})
	}
}

func TestMaterializationInboxRetiresMidReceiveAndDrainsValidEnd(t *testing.T) {
	// Rationale: retirement may race a transfer already partway through the
	// inline queue and must carry its digest forward without retaining content.
	content := []byte("mid-receive private materialization\n")
	assignment := materializationAssignment(t, content)
	inbox := newMaterializationInbox()
	if err := inbox.Register(assignment); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	records := materializationTransfers(assignment, content, 5)
	for _, transfer := range records[:2] {
		if err := inbox.Accept(context.Background(), transfer); err != nil {
			t.Fatalf("Accept(prefix) error = %v", err)
		}
	}
	inbox.mu.Lock()
	step := inbox.tasks[assignment.TaskID].steps[materializationStepID]
	inbox.mu.Unlock()
	inbox.Retire(assignment.TaskID)
	assertRetiredMaterializationStepCleared(t, inbox, assignment.TaskID, materializationStepID)
	if step.digest == nil {
		t.Fatal("Retire() did not retain wipe-capable digest authority")
	}
	for _, transfer := range records[2:] {
		if err := inbox.Accept(context.Background(), transfer); err != nil {
			t.Fatalf("Accept(suffix) error = %v", err)
		}
	}
	if step.digest != nil {
		t.Fatal("valid retired End retained digest state")
	}
	inbox.mu.Lock()
	_, retained := inbox.tasks[assignment.TaskID]
	inbox.mu.Unlock()
	if retained {
		t.Fatal("valid retired End retained task correlation")
	}
}

func TestMaterializationInboxExpiresRetiredTransferWithoutEnd(t *testing.T) {
	// Rationale: a disconnected Controller may never deliver End, so retired
	// correlation and derived digest state must expire at the assignment deadline.
	content := []byte("expiring private materialization\n")
	assignment := materializationAssignment(t, content)
	assignment.Deadline = time.Now().Add(25 * time.Millisecond)
	inbox := newMaterializationInbox()
	if err := inbox.Register(assignment); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	records := materializationTransfers(assignment, content, 5)
	for _, transfer := range records[:2] {
		if err := inbox.Accept(context.Background(), transfer); err != nil {
			t.Fatalf("Accept(prefix) error = %v", err)
		}
	}
	inbox.mu.Lock()
	step := inbox.tasks[assignment.TaskID].steps[materializationStepID]
	inbox.mu.Unlock()
	inbox.Retire(assignment.TaskID)
	waitForMaterializationTaskRemoval(t, inbox, assignment.TaskID)
	if step.digest != nil || step.content != nil {
		t.Fatal("retired expiry retained plaintext or digest state")
	}
	lateChunk := materializationTransfers(assignment, content, len(content))[1]
	if err := inbox.Accept(context.Background(), lateChunk); err == nil {
		t.Fatal("Accept() accepted a post-deadline chunk")
	}
	if len(lateChunk.GetChunk().GetContent()) != 0 {
		t.Fatal("Accept() retained a post-deadline chunk")
	}
}

func TestMaterializationInboxReplacesIdenticalRetiredAssignmentOnReplay(t *testing.T) {
	// Rationale: reconnect can replay the exact assignment after terminal
	// retirement; replacement must wipe the old partial digest without admitting
	// a duplicate while the replacement is active.
	content := []byte("replayed private materialization\n")
	assignment := materializationAssignment(t, content)
	inbox := newMaterializationInbox()
	if err := inbox.Register(assignment); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	records := materializationTransfers(assignment, content, 5)
	for _, transfer := range records[:2] {
		if err := inbox.Accept(context.Background(), transfer); err != nil {
			t.Fatalf("Accept(prefix) error = %v", err)
		}
	}
	inbox.mu.Lock()
	oldTask := inbox.tasks[assignment.TaskID]
	oldStep := oldTask.steps[materializationStepID]
	inbox.mu.Unlock()
	inbox.Retire(assignment.TaskID)
	if err := inbox.Register(assignment); err != nil {
		t.Fatalf("Register(replay) error = %v", err)
	}
	if oldStep.digest != nil || oldStep.content != nil || oldTask.expiry != nil {
		t.Fatal("Register(replay) retained old retirement state")
	}
	inbox.mu.Lock()
	replacement := inbox.tasks[assignment.TaskID]
	inbox.mu.Unlock()
	if replacement == oldTask || replacement.retired {
		t.Fatal("Register(replay) did not install a fresh active inbox")
	}
	if err := inbox.Register(assignment); err == nil {
		t.Fatal("Register() accepted an active duplicate assignment")
	}
	inbox.Release(assignment.TaskID)
}

func TestMaterializationInboxDestroysRetiredHasherOnFailureAndRelease(t *testing.T) {
	// Rationale: every non-success terminal path must call the canonical Hasher
	// destruction lifecycle and drop the interface reference.
	content := []byte("destroyed private materialization\n")
	for _, test := range []struct {
		name   string
		finish func(*materializationInbox, Assignment, []*agentpb.MaterializationTransfer) error
	}{
		{name: "failure", finish: func(
			inbox *materializationInbox,
			_ Assignment,
			records []*agentpb.MaterializationTransfer,
		) error {
			records[2].GetChunk().Sequence++
			return inbox.Accept(context.Background(), records[2])
		}},
		{name: "hard release", finish: func(
			inbox *materializationInbox,
			assignment Assignment,
			_ []*agentpb.MaterializationTransfer,
		) error {
			inbox.Release(assignment.TaskID)
			return nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			assignment := materializationAssignment(t, content)
			inbox := newMaterializationInbox()
			if err := inbox.Register(assignment); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
			records := materializationTransfers(assignment, content, 5)
			for _, transfer := range records[:2] {
				if err := inbox.Accept(context.Background(), transfer); err != nil {
					t.Fatalf("Accept(prefix) error = %v", err)
				}
			}
			inbox.mu.Lock()
			step := inbox.tasks[assignment.TaskID].steps[materializationStepID]
			inbox.mu.Unlock()
			inbox.Retire(assignment.TaskID)
			if step.digest == nil {
				t.Fatal("Retire() did not install digest state")
			}
			if err := test.finish(inbox, assignment, records); test.name == "failure" && err == nil {
				t.Fatal("Accept() accepted malformed retired chunk")
			}
			if step.digest != nil || step.content != nil {
				t.Fatal("terminal path retained plaintext or digest state")
			}
		})
	}
}

func TestMaterializationInboxRejectsChunkThirtyThreeAndClearsIt(t *testing.T) {
	// Rationale: the 1 MiB transfer contract permits at most 32 chunks even
	// when smaller chunks would remain under the byte ceiling.
	content := bytes.Repeat([]byte{'x'}, 33)
	assignment := materializationAssignment(t, content)
	inbox := newMaterializationInbox()
	if err := inbox.Register(assignment); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	records := materializationTransfers(assignment, content, 1)
	for _, transfer := range records[:33] {
		if err := inbox.Accept(context.Background(), transfer); err != nil {
			t.Fatalf("Accept(first 32 chunks) error = %v", err)
		}
	}
	chunkThirtyThree := records[33]
	if err := inbox.Accept(context.Background(), chunkThirtyThree); err == nil {
		t.Fatal("Accept() accepted materialization chunk 33")
	}
	if len(chunkThirtyThree.GetChunk().GetContent()) != 0 {
		t.Fatal("Accept() retained rejected chunk 33 plaintext")
	}
}

func waitForMaterializationTaskRemoval(t *testing.T, inbox *materializationInbox, taskID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		inbox.mu.Lock()
		_, retained := inbox.tasks[taskID]
		inbox.mu.Unlock()
		if !retained {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("materialization task did not expire")
}

func assertRetiredMaterializationStepCleared(
	t *testing.T,
	inbox *materializationInbox,
	taskID string,
	stepID string,
) {
	t.Helper()
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	task := inbox.tasks[taskID]
	if task == nil {
		t.Fatal("retired materialization task correlation is missing")
	}
	step := task.steps[stepID]
	if step == nil || !step.retired {
		t.Fatal("materialization step is not retired")
	}
	if step.content != nil {
		t.Fatal("retired materialization step retained plaintext")
	}
}

type decodingMaterializationHelper struct {
	volumeDir string
	header    entrymaterialization.Header
	content   []byte
	err       error
}

func (helper *decodingMaterializationHelper) Run(
	ctx context.Context,
	request materializerrunner.Request,
) error {
	helper.volumeDir = request.VolumeDir
	header, err := entrymaterialization.Decode(
		ctx,
		request.Stream,
		entrymaterialization.Limits{
			MaxContentBytes:     entrymaterialization.MaximumContentBytes,
			MaxDestinationBytes: entrymaterialization.MaximumDestinationBytes,
		},
		func(_ context.Context, _ entrymaterialization.Header, source io.Reader) error {
			content, readErr := io.ReadAll(source)
			helper.content = append([]byte(nil), content...)
			return readErr
		},
	)
	helper.header = header
	if err != nil {
		return err
	}
	return helper.err
}

func materializationAssignment(t *testing.T, content []byte) Assignment {
	t.Helper()
	yaml := []byte("services: {}\n")
	yamlDigest := sha256.Sum256(yaml)
	contentDigest := sha256.Sum256(content)
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: materializationPlanID, RenderGeneration: 7,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
		TargetId:  materializationEnvironmentID,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId: materializationArtifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
			OwnerId: materializationEnvironmentID, ProjectName: "gp-" + strings.ToLower(materializationEnvironmentID),
			CanonicalYaml: yaml, YamlSha256: yamlDigest[:], AuthorizedVolumeDir: materializationVolumeDir,
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: materializationStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_MaterializeFile{MaterializeFile: &agentpb.MaterializeFile{
				ArtifactId: materializationArtifactID, MaterializationId: materializationID,
				EnvironmentId: materializationEnvironmentID,
				Destination:   "blueprints/" + materializationPlanID + "/blueprint.yaml",
				OutputKind:    agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE,
				Mode:          0o444, Length: uint64(len(content)), Sha256: contentDigest[:],
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	deadline := time.Now().Add(time.Minute)
	return Assignment{
		AssignmentID: workerTestAssignmentID,
		TaskID:       materializationTaskID, OperationID: materializationOperationID,
		Plan: plan, Deadline: deadline, ExecutionEpoch: 1,
		ExecutionMode:   agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		ForwardDeadline: deadline, RecoveryDeadline: deadline.Add(time.Minute),
	}
}

func twoMaterializationAssignment(t *testing.T, firstContent []byte, secondContent []byte) Assignment {
	t.Helper()
	assignment := materializationAssignment(t, firstContent)
	plan := proto.Clone(assignment.Plan).(*agentpb.ExecutionPlan)
	second := proto.Clone(plan.GetSteps()[0]).(*agentpb.ExecutionStep)
	second.StepId = materializationSecondStepID
	secondMaterialization := second.GetMaterializeFile()
	secondDigest := sha256.Sum256(secondContent)
	secondMaterialization.MaterializationId = materializationSecondID
	secondMaterialization.Destination = "blueprints/" + materializationPlanID + "/late.yaml"
	secondMaterialization.Length = uint64(len(secondContent))
	secondMaterialization.Sha256 = secondDigest[:]
	plan.Steps = append(plan.Steps, second)
	plan.PlanHash = nil
	sealed, err := executionplan.Seal(plan)
	if err != nil {
		t.Fatalf("Seal(two materializations) error = %v", err)
	}
	assignment.Plan = sealed
	return assignment
}

func materializationTransfers(
	assignment Assignment,
	content []byte,
	chunkBytes int,
) []*agentpb.MaterializationTransfer {
	return materializationTransfersForStep(assignment, 0, content, chunkBytes)
}

func materializationTransfersForStep(
	assignment Assignment,
	stepIndex int,
	content []byte,
	chunkBytes int,
) []*agentpb.MaterializationTransfer {
	step := assignment.Plan.Steps[stepIndex]
	materialization := step.GetMaterializeFile()
	planHash := hashForPlan(assignment.Plan)
	outer := func() *agentpb.MaterializationTransfer {
		return &agentpb.MaterializationTransfer{
			TaskId: assignment.TaskID, AssignmentId: assignment.AssignmentID,
			PlanHash: append([]byte(nil), planHash[:]...), StepId: step.StepId,
		}
	}
	headerRecord := outer()
	headerRecord.Record = &agentpb.MaterializationTransfer_Header{
		Header: &agentpb.MaterializationTransferHeader{
			ArtifactId: materialization.ArtifactId, MaterializationId: materialization.MaterializationId,
			EnvironmentId: materialization.EnvironmentId, RenderGeneration: assignment.Plan.RenderGeneration,
			Destination: materialization.Destination, ServiceId: materialization.ServiceId,
			ServiceName: materialization.ServiceName,
			OutputKind:  materialization.OutputKind, Uid: materialization.Uid, Gid: materialization.Gid,
			Mode: materialization.Mode, Length: materialization.Length,
			Sha256: append([]byte(nil), materialization.Sha256...),
		},
	}
	records := []*agentpb.MaterializationTransfer{headerRecord}
	var sequence uint32
	for offset := 0; offset < len(content); offset += chunkBytes {
		end := min(offset+chunkBytes, len(content))
		sequence++
		chunkRecord := outer()
		chunkRecord.Record = &agentpb.MaterializationTransfer_Chunk{
			Chunk: &agentpb.MaterializationTransferChunk{
				Sequence: sequence, Content: append([]byte(nil), content[offset:end]...),
			},
		}
		records = append(records, chunkRecord)
	}
	endRecord := outer()
	endRecord.Record = &agentpb.MaterializationTransfer_End{
		End: &agentpb.MaterializationTransferEnd{ChunkCount: sequence},
	}
	return append(records, endRecord)
}
