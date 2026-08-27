package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	materializationTaskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationOperationID   = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationPlanID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationStepID        = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationArtifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationID            = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
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
	runtime, err := NewMaterializationRuntime(helper)
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
	runtime, err := NewMaterializationRuntime(helper)
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

type decodingMaterializationHelper struct {
	volumeDir string
	header    entrymaterialization.Header
	content   []byte
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
	return err
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
	return Assignment{
		AssignmentID: workerTestAssignmentID,
		TaskID:       materializationTaskID, OperationID: materializationOperationID,
		Plan: plan, Deadline: time.Now().Add(time.Minute),
	}
}

func materializationTransfers(
	assignment Assignment,
	content []byte,
	chunkBytes int,
) []*agentpb.MaterializationTransfer {
	step := assignment.Plan.Steps[0]
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
