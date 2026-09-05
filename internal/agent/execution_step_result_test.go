package agent

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestScriptProjectionRequiresAcknowledgedProcedureImageResultBeforeStart(t *testing.T) {
	authority := &agentpb.ProcedureServiceImageAuthority{
		ComposeApplyStepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ArtifactId:         "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ServiceId:          "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ReleaseId:          "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RequestedReference: "registry.example/app:candidate",
	}
	snapshot := &agentpb.ResolvedRunnerSnapshot{ProcedureServiceImage: authority}
	projection := &agentpb.ScriptRunnerProjection{Image: authority.RequestedReference}
	assignment := Assignment{OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV", Plan: &agentpb.ExecutionPlan{
		PlanHash: bytes.Repeat([]byte{0xaa}, 32),
	}}
	if _, err := acknowledgedScriptRunnerProjection(assignment, snapshot, projection); !errors.Is(
		err, errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("missing result error = %v, want state conflict before Script start", err)
	}
	result := &agentpb.ExecutionStepResult{
		OperationId: assignment.OperationID, PlanHash: append([]byte(nil), assignment.Plan.PlanHash...),
		StepId: authority.ComposeApplyStepId,
		Result: &agentpb.ExecutionStepResult_ProcedureServiceImage{
			ProcedureServiceImage: &agentpb.ProcedureServiceImageResult{
				ServiceId: authority.ServiceId, ReleaseId: authority.ReleaseId,
				RequestedReference: authority.RequestedReference,
				ImmutableReference: "registry.example/app@sha256:" + strings.Repeat("b", 64),
				ImageDigest:        bytes.Repeat([]byte{0xbb}, 32),
				LocalImageId:       "sha256:" + strings.Repeat("c", 64),
			},
		},
	}
	assignment.AcknowledgedStepResults = []*agentpb.ExecutionStepResult{result}
	resolved, err := acknowledgedScriptRunnerProjection(assignment, snapshot, projection)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.GetImage() != result.GetProcedureServiceImage().GetImmutableReference() ||
		projection.GetImage() != authority.RequestedReference {
		t.Fatalf("resolved/original image = %q/%q", resolved.GetImage(), projection.GetImage())
	}
	conflict := proto.CloneOf(result)
	conflict.Result = &agentpb.ExecutionStepResult_ProcedureServiceImage{
		ProcedureServiceImage: &agentpb.ProcedureServiceImageResult{
			ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		},
	}
	assignment.AcknowledgedStepResults = []*agentpb.ExecutionStepResult{conflict}
	if _, err := acknowledgedScriptRunnerProjection(assignment, snapshot, projection); !errors.Is(
		err, errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("conflicting result error = %v, want state conflict", err)
	}
}

func TestComposeApplyAcknowledgementInstallsResultBeforeImmediateRunScript(t *testing.T) {
	assignment, step, _ := procedureComposeAssignment(t)
	helper := completedComposeHelper()
	observer := &fakeComposeObserver{image: procedureImageEvidence(), projects: []*agentpb.ObservedProject{{ProjectName: "gp-platform"}}}
	runtime, err := NewComposeRuntime(helper, observer)
	if err != nil {
		t.Fatal(err)
	}
	composed, err := runtime.executeStep(context.Background(), assignment, step)
	if err != nil || composed.ExecutionStepResult == nil {
		t.Fatalf("ComposeApply result = %#v, %v", composed, err)
	}
	pool := &WorkerPool{
		outputs:              make(chan WorkerOutput, 1),
		executionStepResults: newExecutionStepResultInbox(),
	}
	done := make(chan error, 1)
	go func() {
		done <- pool.CheckpointExecutionStepResult(context.Background(), &assignment, composed.ExecutionStepResult)
	}()
	output := <-pool.outputs
	request := output.ExecutionStepResult
	if request == nil {
		t.Fatal("checkpoint did not emit result")
	}
	ack := &agentpb.ExecutionStepResultAck{
		TaskId: request.GetTaskId(), AssignmentId: request.GetAssignmentId(),
		OperationId: request.GetResult().GetOperationId(), PlanHash: request.GetResult().GetPlanHash(),
		StepId:               request.GetResult().GetStepId(),
		ControlPayloadSha256: request.GetResult().GetControlPayloadSha256(),
	}
	if err := pool.AcceptExecutionStepResultAck(ack); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	snapshot := assignment.Plan.ScriptRunnerSnapshots[0]
	projection := &agentpb.ScriptRunnerProjection{Image: snapshot.ProcedureServiceImage.RequestedReference}
	resolved, err := acknowledgedScriptRunnerProjection(assignment, snapshot, projection)
	if err != nil || resolved.GetImage() != procedureImageEvidence().GetImmutableReference() {
		t.Fatalf("immediate RunScript authority = %q, %v", resolved.GetImage(), err)
	}
}

func procedureComposeAssignment(t *testing.T) (Assignment, *agentpb.ExecutionStep, *agentpb.ExecutionStepResult) {
	t.Helper()
	const (
		operationID = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		stepID      = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		artifactID  = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		serviceID   = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		releaseID   = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		requested   = "registry.example/app:candidate"
	)
	planHash := bytes.Repeat([]byte{0xaa}, 32)
	step := &agentpb.ExecutionStep{StepId: stepID, Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
		ArtifactId: artifactID, ServiceIds: []string{serviceID},
	}}}
	authority := &agentpb.ProcedureServiceImageAuthority{
		ComposeApplyStepId: stepID, ArtifactId: artifactID, ServiceId: serviceID, ReleaseId: releaseID, RequestedReference: requested,
	}
	plan := &agentpb.ExecutionPlan{
		PlanHash: planHash,
		Steps:    []*agentpb.ExecutionStep{step},
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId: artifactID,
			Services: []*agentpb.ComposeService{{
				ServiceId: serviceID, ImageReference: requested,
				ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.release-id", Value: releaseID}},
			}},
		}},
		ScriptRunnerSnapshots: []*agentpb.ResolvedRunnerSnapshot{{ProcedureServiceImage: authority}},
	}
	evidence, err := executionplan.SealExecutionStepResult(&agentpb.ExecutionStepResult{
		OperationId: operationID, PlanHash: planHash, StepId: stepID,
		Result: &agentpb.ExecutionStepResult_ProcedureServiceImage{ProcedureServiceImage: procedureImageEvidence()},
	})
	if err != nil {
		t.Fatal(err)
	}
	return Assignment{
		AssignmentID: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV", TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		OperationID: operationID, Plan: plan,
	}, step, evidence
}

func procedureImageEvidence() *agentpb.ProcedureServiceImageResult {
	return &agentpb.ProcedureServiceImageResult{
		ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", ReleaseId: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RequestedReference: "registry.example/app:candidate",
		ImmutableReference: "registry.example/app@sha256:" + strings.Repeat("b", 64),
		ImageDigest:        bytes.Repeat([]byte{0xbb}, 32), LocalImageId: "sha256:" + strings.Repeat("c", 64),
	}
}
