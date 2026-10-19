package materialization

import (
	"context"
	"crypto/sha256"
	"io"
	"strings"
	"testing"
	"time"

	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	materializationTestAssignmentID = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationTaskID           = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationOperationID      = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationPlanID           = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationStepID           = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationSecondStepID     = "step_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	materializationArtifactID       = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationID               = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	materializationSecondID         = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	materializationEnvironmentID    = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationVolumeDir        = "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
		"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

type decodingMaterializationHelper struct {
	volumeDir string
	header    entrymaterialization.Header
	content   []byte
	err       error
}

func (helper *decodingMaterializationHelper) StopCaptured(context.Context, materializerrunner.Ownership) error {
	return nil
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
