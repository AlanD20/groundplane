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

	filematerialization "github.com/AlanD20/groundplane/internal/agent/materialization"
	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
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

func TestWorkerExecutesMaterializationAfterVerifiedTransfer(t *testing.T) {
	// Rationale: the worker must reserve the task before accepting bytes, block
	// only that task's step, and resume through the ordinary result path after verification.
	content := []byte("services: {}\n")
	assignment := workerMaterializationAssignment(t, content)
	helper := &workerDecodingMaterializationHelper{}
	runtime, err := filematerialization.New(helper, nil)
	if err != nil {
		t.Fatalf("filematerialization.New() error = %v", err)
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
	for _, transfer := range workerMaterializationTransfers(assignment, content, 4) {
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
	assignment := workerTwoMaterializationAssignment(t, firstContent, lateContent)
	helper := &workerDecodingMaterializationHelper{err: errors.New("stop after first materialization")}
	runtime, err := filematerialization.New(helper, nil)
	if err != nil {
		t.Fatalf("filematerialization.New() error = %v", err)
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
	for _, transfer := range workerMaterializationTransfersForStep(assignment, 0, firstContent, 7) {
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
	if _, err := pool.materializations.Take(context.Background(), assignment.TaskID, materializationSecondStepID); err == nil {
		t.Fatal("retired materialization remained consumable")
	}

	lateTransfers := workerMaterializationTransfersForStep(assignment, 1, lateContent, 5)
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
			if _, err := pool.materializations.Take(context.Background(), assignment.TaskID, materializationSecondStepID); err == nil {
				t.Fatal("retired materialization became consumable while draining")
			}
		}
	}
	if err := pool.AcceptMaterializationTransfer(ctx, lateTransfers[len(lateTransfers)-1]); err == nil {
		cancel()
		<-done
		t.Fatal("materialization retirement accepted a record after valid End")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WorkerPool.Run() did not stop")
	}
}

type workerDecodingMaterializationHelper struct {
	volumeDir string
	header    entrymaterialization.Header
	content   []byte
	err       error
}

func (helper *workerDecodingMaterializationHelper) Run(
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

func workerMaterializationAssignment(t *testing.T, content []byte) testtaskassignment.Assignment {
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
	return testtaskassignment.Assignment{
		AssignmentID: workerTestAssignmentID,
		TaskID:       materializationTaskID, OperationID: materializationOperationID,
		Plan: plan, Deadline: deadline, ExecutionEpoch: 1,
		ExecutionMode:   agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		ForwardDeadline: deadline, RecoveryDeadline: deadline.Add(time.Minute),
	}
}

func workerTwoMaterializationAssignment(
	t *testing.T,
	firstContent []byte,
	secondContent []byte,
) testtaskassignment.Assignment {
	t.Helper()
	assignment := workerMaterializationAssignment(t, firstContent)
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

func workerMaterializationTransfers(
	assignment testtaskassignment.Assignment,
	content []byte,
	chunkBytes int,
) []*agentpb.MaterializationTransfer {
	return workerMaterializationTransfersForStep(assignment, 0, content, chunkBytes)
}

func workerMaterializationTransfersForStep(
	assignment testtaskassignment.Assignment,
	stepIndex int,
	content []byte,
	chunkBytes int,
) []*agentpb.MaterializationTransfer {
	step := assignment.Plan.Steps[stepIndex]
	materialization := step.GetMaterializeFile()
	planHash := testtaskassignment.PlanDigest(assignment.Plan)
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
