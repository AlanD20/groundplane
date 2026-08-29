package agent

import (
	"context"
	"crypto/sha256"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestDockerScriptRuntimeCheckpointsBeforeEachSideEffect(t *testing.T) {
	t.Parallel()

	body := []byte("echo migration\n")
	bodyDigest := sha256.Sum256(body)
	planDigest := sha256.Sum256([]byte("sealed plan"))
	executionID := ids.NewULID()
	snapshotID := ids.NewULID()
	scriptID := ids.New(ids.KindScript)
	stepID := ids.New(ids.KindStep)
	assignment := Assignment{
		AssignmentID: ids.New(ids.KindAssignment),
		TaskID:       ids.New(ids.KindTask),
		OperationID:  ids.New(ids.KindOperation),
		Plan: &agentpb.ExecutionPlan{
			PlanHash:                append([]byte(nil), planDigest[:]...),
			ScriptRunnerProjections: []*agentpb.ScriptRunnerProjection{{SnapshotId: snapshotID}},
		},
		ScriptArtifacts: &agentpb.ScriptAssignmentArtifacts{
			Bodies: []*agentpb.ScriptBodyArtifact{{
				Metadata: &agentpb.ScriptBodyArtifactMetadata{
					ScriptExecutionId: executionID, ScriptId: scriptID, Generation: 1,
					Size: uint32(len(body)), Sha256: append([]byte(nil), bodyDigest[:]...),
				},
				Body: append([]byte(nil), body...),
			}},
		},
		ScriptCheckpoint: &agentpb.ScriptExecutionCheckpoint{
			State: agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED,
		},
		Deadline: time.Now().Add(time.Minute),
	}
	step := &agentpb.ExecutionStep{
		StepId: stepID,
		Payload: &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{
			ScriptExecutionId: executionID, ScriptId: scriptID, ScriptGeneration: 1,
			RunnerSnapshotId: snapshotID,
		}},
	}

	events := make([]string, 0, 10)
	engine := &checkpointOrderScriptEngine{events: &events, bodyDigest: bodyDigest}
	runtime, err := NewDockerScriptRuntime(engine)
	if err != nil {
		t.Fatalf("create Script runtime: %v", err)
	}
	checkpoint := func(_ context.Context, request *agentpb.ScriptCheckpointRequest) error {
		if _, err := executionplan.ValidateScriptCheckpointRequest(request); err != nil {
			return err
		}
		events = append(events, "checkpoint:"+request.State.String())
		return nil
	}
	if exitCode, err := runtime.ExecuteScript(context.Background(), assignment, step, checkpoint); err != nil || exitCode != 0 {
		t.Fatalf("execute Script: exit=%d err=%v", exitCode, err)
	}

	want := []string{
		"checkpoint:SCRIPT_EXECUTION_STATE_START_AUTHORIZED",
		"engine:prepare",
		"checkpoint:SCRIPT_EXECUTION_STATE_BODY_PREPARED",
		"engine:recover",
		"engine:create",
		"checkpoint:SCRIPT_EXECUTION_STATE_CONTAINER_CREATED",
		"engine:run",
		"checkpoint:SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED",
		"engine:cleanup",
		"checkpoint:SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN",
	}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

type checkpointOrderScriptEngine struct {
	events     *[]string
	bodyDigest [sha256.Size]byte
}

func (engine *checkpointOrderScriptEngine) PrepareBody(
	context.Context,
	scriptexecution.Request,
) (scriptexecution.BodyEvidence, error) {
	*engine.events = append(*engine.events, "engine:prepare")
	return scriptexecution.BodyEvidence{
		SHA256: append([]byte(nil), engine.bodyDigest[:]...), UID: 1000, GID: 1000,
		Device: 1, Inode: 2, Leaf: "body",
	}, nil
}

func (engine *checkpointOrderScriptEngine) RecoverContainer(
	context.Context,
	scriptexecution.Request,
	scriptexecution.BodyEvidence,
) (scriptexecution.ContainerRecovery, error) {
	*engine.events = append(*engine.events, "engine:recover")
	return scriptexecution.ContainerRecovery{}, nil
}

func (engine *checkpointOrderScriptEngine) CreateContainer(
	context.Context,
	scriptexecution.Request,
	scriptexecution.BodyEvidence,
) (scriptexecution.ContainerEvidence, error) {
	*engine.events = append(*engine.events, "engine:create")
	return scriptexecution.ContainerEvidence{
		ID: strings.Repeat("a", 64), OwnershipLabelsSHA256: bytesOf(3, sha256.Size),
	}, nil
}

func (engine *checkpointOrderScriptEngine) RunContainer(
	context.Context,
	scriptexecution.Request,
	scriptexecution.BodyEvidence,
	scriptexecution.ContainerEvidence,
) (scriptexecution.RunResult, error) {
	*engine.events = append(*engine.events, "engine:run")
	return scriptexecution.RunResult{}, nil
}

func (engine *checkpointOrderScriptEngine) Cleanup(
	_ scriptexecution.Request,
	body *scriptexecution.BodyEvidence,
	container *scriptexecution.ContainerEvidence,
) (scriptexecution.CleanupProof, error) {
	*engine.events = append(*engine.events, "engine:cleanup")
	return scriptexecution.CleanupProof{
		ContainerID: container.ID, BodyDevice: body.Device, BodyInode: body.Inode, BodyLeaf: body.Leaf,
		ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true,
	}, nil
}

func (engine *checkpointOrderScriptEngine) Close() error { return nil }

func bytesOf(value byte, size int) []byte {
	result := make([]byte, size)
	for index := range result {
		result[index] = value
	}
	return result
}
