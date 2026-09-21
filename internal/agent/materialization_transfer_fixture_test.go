package agent

import (
	sha256 "crypto/sha256"
	strings "strings"
	testing "testing"
	time "time"

	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	executionplan "github.com/AlanD20/groundplane/internal/common/executionplan"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

func materializationAssignment(t *testing.T, content []byte) testtaskassignment.Assignment {
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
		AssignmentID: materializationTestAssignmentID,
		TaskID:       materializationTaskID, OperationID: materializationOperationID,
		Plan: plan, Deadline: deadline, ExecutionEpoch: 1,
		ExecutionMode:   agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		ForwardDeadline: deadline, RecoveryDeadline: deadline.Add(time.Minute),
	}
}

const (
	materializationTestAssignmentID = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func materializationTransfersForStep(
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
