package scriptruntime

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// QA: TASK-25. These are runtime behavior checks, not transport durability proof.
// Rationale: a Script consumes its presealed local image without a preceding
// Compose mutation or image-result ACK, while retaining every execution checkpoint.
func TestDockerScriptRuntimeCheckpointsBeforeEachSideEffect(t *testing.T) {
	t.Parallel()
	const localImageID = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	invocation, bodyDigest := capturedContainerFailureFixture()

	events := make([]string, 0, 10)
	engine := &checkpointOrderScriptEngine{events: &events, bodyDigest: bodyDigest}
	runtime, err := New(engine)
	if err != nil {
		t.Fatalf("create Script runtime: %v", err)
	}
	checkpoint := func(_ context.Context, request *agentpb.ScriptCheckpointRequest) error {
		if _, err := executionplan.ComputeScriptCheckpointPayloadDigest(request); err != nil {
			return err
		}
		events = append(events, "checkpoint:"+request.State.String())
		return nil
	}
	if exitCode, err := runtime.ExecuteScript(context.Background(), invocation, checkpoint); err != nil ||
		exitCode != 0 {
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
	if engine.createdImage != localImageID {
		t.Fatalf("runner created with image %q, want presealed local image %q", engine.createdImage, localImageID)
	}
}

func TestDockerScriptRuntimePreservesRunFailureAfterTypedCheckpoint(t *testing.T) {
	t.Parallel()
	invocation, bodyDigest := capturedContainerFailureFixture()
	sentinel := errors.New("connect secondary network gp_net_secondary: endpoint denied")
	events := make([]string, 0, 10)
	runtime, err := New(&checkpointOrderScriptEngine{
		events: &events, bodyDigest: bodyDigest, runErr: sentinel,
	})
	if err != nil {
		t.Fatal(err)
	}
	var outcome agentpb.ScriptOutcomeReason
	checkpoint := func(_ context.Context, request *agentpb.ScriptCheckpointRequest) error {
		if _, err := executionplan.ComputeScriptCheckpointPayloadDigest(request); err != nil {
			return err
		}
		if request.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED {
			outcome = request.GetOutcome().GetReason()
		}
		return nil
	}
	_, err = runtime.ExecuteScript(context.Background(), invocation, checkpoint)
	if !errors.Is(err, sentinel) {
		t.Fatalf("ExecuteScript() error = %v, want wrapped runtime cause", err)
	}
	if outcome != agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RUNTIME_FAILURE {
		t.Fatalf("durable Script outcome = %s", outcome)
	}
}

// Rationale: once Docker returns a container id, a pre-start create failure
// must checkpoint and clean that exact container instead of proving an empty handle absent.
func TestDockerScriptRuntimeCheckpointsCreateEvidenceBeforeFailureOutcome(t *testing.T) {
	t.Parallel()

	invocation, bodyDigest := capturedContainerFailureFixture()
	createErr := errors.New("created container failed validation")
	events := make([]string, 0, 10)
	engine := &checkpointOrderScriptEngine{events: &events, bodyDigest: bodyDigest, createErr: createErr}
	runtime, err := New(engine)
	if err != nil {
		t.Fatal(err)
	}
	var outcome agentpb.ScriptOutcomeReason
	checkpoint := func(_ context.Context, request *agentpb.ScriptCheckpointRequest) error {
		if _, err := executionplan.ComputeScriptCheckpointPayloadDigest(request); err != nil {
			return err
		}
		events = append(events, "checkpoint:"+request.State.String())
		if request.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED {
			outcome = request.GetOutcome().GetReason()
		}
		return nil
	}
	if _, err := runtime.ExecuteScript(context.Background(), invocation, checkpoint); err == nil {
		t.Fatal("expected create validation failure")
	}
	want := []string{
		"checkpoint:SCRIPT_EXECUTION_STATE_START_AUTHORIZED",
		"engine:prepare",
		"checkpoint:SCRIPT_EXECUTION_STATE_BODY_PREPARED",
		"engine:recover",
		"engine:create",
		"checkpoint:SCRIPT_EXECUTION_STATE_CONTAINER_CREATED",
		"checkpoint:SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED",
		"engine:cleanup",
		"checkpoint:SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN",
	}
	containerID := strings.Repeat("a", 64)
	if !slices.Equal(events, want) ||
		outcome != agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_START_FAILURE ||
		engine.cleanupContainerID != containerID {
		t.Fatalf("events/outcome/cleanup id = %v/%s/%q, want %v/start failure/%q",
			events, outcome, engine.cleanupContainerID, want, containerID)
	}
}

// Rationale: a cleanup failure must not mask the original create failure after
// the captured container evidence and failure outcome are durably checkpointed.
func TestDockerScriptRuntimeJoinsCreateAndCleanupFailures(t *testing.T) {
	t.Parallel()

	invocation, bodyDigest := capturedContainerFailureFixture()
	createErr := errors.New("created container failed validation")
	cleanupErr := errors.New("remove captured container failed")
	engine := &checkpointOrderScriptEngine{
		events: &[]string{}, bodyDigest: bodyDigest, createErr: createErr, cleanupErr: cleanupErr,
	}
	runtime, err := New(engine)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := func(_ context.Context, request *agentpb.ScriptCheckpointRequest) error {
		_, checkpointErr := executionplan.ComputeScriptCheckpointPayloadDigest(request)
		return checkpointErr
	}

	_, err = runtime.ExecuteScript(context.Background(), invocation, checkpoint)
	if !errors.Is(err, createErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("ExecuteScript() error = %v, want joined create and cleanup causes", err)
	}
}

// Rationale: a captured container changes cancellation to abort and makes
// ownership/state conflicts authoritative over a concurrent cancellation.
func TestDockerScriptRuntimePreservesCapturedContainerFailurePrecedence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		createErr error
		want      agentpb.ScriptOutcomeReason
	}{
		{name: "ownership mismatch", createErr: errs.New(errs.KindStateConflict, "ownership mismatch"),
			want: agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE},
		{name: "running state", createErr: errs.New(errs.KindStateConflict, "running state"),
			want: agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE},
		{name: "cancellation", createErr: context.Canceled,
			want: agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			invocation, bodyDigest := capturedContainerFailureFixture()
			ctx, cancel := context.WithCancel(context.Background())
			engine := &checkpointOrderScriptEngine{
				events: &[]string{}, bodyDigest: bodyDigest, createErr: test.createErr, cancelCreate: cancel,
			}
			runtime, err := New(engine)
			if err != nil {
				t.Fatal(err)
			}
			states := make([]agentpb.ScriptExecutionState, 0, 5)
			var outcome agentpb.ScriptOutcomeReason
			checkpoint := func(_ context.Context, request *agentpb.ScriptCheckpointRequest) error {
				if _, err := executionplan.ComputeScriptCheckpointPayloadDigest(request); err != nil {
					return err
				}
				states = append(states, request.State)
				if request.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED {
					outcome = request.GetOutcome().GetReason()
				}
				return nil
			}
			if _, err := runtime.ExecuteScript(ctx, invocation, checkpoint); err == nil {
				t.Fatal("expected captured-container failure")
			}
			wantStates := []agentpb.ScriptExecutionState{
				agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED,
				agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED,
				agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED,
				agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED,
				agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN,
			}
			containerID := strings.Repeat("a", 64)
			if !slices.Equal(states, wantStates) || outcome != test.want || engine.cleanupContainerID != containerID {
				t.Fatalf("states/outcome/cleanup id = %v/%s/%q, want %v/%s/%q",
					states, outcome, engine.cleanupContainerID, wantStates, test.want, containerID)
			}
		})
	}
}

func capturedContainerFailureFixture() (Invocation, [sha256.Size]byte) {
	body := []byte("echo migration\n")
	bodyDigest := sha256.Sum256(body)
	at := time.Now().UTC()
	executionID, snapshotID := ids.NewULID(), ids.NewULID()
	scriptID := ids.New(ids.KindScript)
	input := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("d", 64)}
	return Invocation{
		Attempt: workexecution.Attempt{
			ID: ids.New(ids.KindAttempt), WorkItemID: ids.New(ids.KindWorkItem), TaskID: ids.New(ids.KindTask),
			Number: 1, Executor: workexecution.Identity{Executor: workexecution.Agent,
				HostID: ids.New(ids.KindHost), AgentID: ids.New(ids.KindAgent), Generation: 1},
			Direction: workexecution.Forward, Input: input, AssignedAt: at, StartedAt: &at, Deadline: at.Add(time.Minute),
		},
		OperationID: ids.New(ids.KindOperation), Input: input,
		Run: &agentpb.RunScript{
			ScriptExecutionId: executionID,
			ScriptId:          scriptID,
			ScriptGeneration:  1,
			RunnerSnapshotId:  snapshotID,
		},
		Snapshot: &agentpb.ResolvedRunnerSnapshot{
			SnapshotId:   snapshotID,
			LocalImageId: "sha256:" + strings.Repeat("b", 64),
		},
		Projection: &agentpb.ScriptRunnerProjection{SnapshotId: snapshotID, Image: "sha256:" + strings.Repeat("b", 64)},
		Body: &agentpb.ScriptBodyArtifact{
			Metadata: &agentpb.ScriptBodyArtifactMetadata{
				ScriptExecutionId: executionID,
				ScriptId:          scriptID,
				Generation:        1,
				Size:              uint32(len(body)),
				Sha256:            append([]byte(nil), bodyDigest[:]...),
			},
			Body: append([]byte(nil), body...),
		},
		Checkpoint: &agentpb.ScriptExecutionCheckpoint{
			ScriptExecutionId: executionID,
			State:             agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED,
		},
	}, bodyDigest
}

func TestDockerScriptRuntimeCompletesNoServingReleaseWithoutStarting(t *testing.T) {
	t.Parallel()
	invocation, bodyDigest := capturedContainerFailureFixture()
	events := []string{}
	runtime, err := New(&checkpointOrderScriptEngine{events: &events, bodyDigest: bodyDigest})
	if err != nil {
		t.Fatal(err)
	}
	var outcome agentpb.ScriptOutcomeReason
	checkpoint := func(_ context.Context, request *agentpb.ScriptCheckpointRequest) error {
		if _, err := executionplan.ComputeScriptCheckpointPayloadDigest(request); err != nil {
			return err
		}
		events = append(events, "checkpoint:"+request.State.String())
		if request.State == agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED {
			outcome = request.GetOutcome().GetReason()
		}
		return nil
	}
	if err := runtime.CompleteScriptWithoutStart(
		context.Background(), invocation,
		agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NO_SERVING_RELEASE, checkpoint,
	); err != nil {
		t.Fatalf("CompleteScriptWithoutStart() error = %v", err)
	}
	want := []string{
		"checkpoint:SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED",
		"engine:cleanup",
		"checkpoint:SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN",
	}
	if outcome != agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NO_SERVING_RELEASE ||
		!slices.Equal(events, want) {
		t.Fatalf("outcome/events = %s/%v, want no-serving/%v", outcome, events, want)
	}
}

type checkpointOrderScriptEngine struct {
	createdImage       string
	events             *[]string
	bodyDigest         [sha256.Size]byte
	createErr          error
	runErr             error
	cleanupErr         error
	cleanupContainerID string
	cancelCreate       context.CancelFunc
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
	_ context.Context,
	request scriptexecution.Request,
	_ scriptexecution.BodyEvidence,
) (scriptexecution.ContainerEvidence, error) {
	engine.createdImage = request.Projection.GetImage()
	*engine.events = append(*engine.events, "engine:create")
	if engine.cancelCreate != nil {
		engine.cancelCreate()
	}
	return scriptexecution.ContainerEvidence{
		ID: strings.Repeat("a", 64), OwnershipLabelsSHA256: bytesOf(3, sha256.Size),
	}, engine.createErr
}

func (engine *checkpointOrderScriptEngine) RunContainer(
	context.Context,
	scriptexecution.Request,
	scriptexecution.BodyEvidence,
	scriptexecution.ContainerEvidence,
) (scriptexecution.RunResult, error) {
	*engine.events = append(*engine.events, "engine:run")
	return scriptexecution.RunResult{}, engine.runErr
}

func (engine *checkpointOrderScriptEngine) Cleanup(
	_ scriptexecution.CleanupRequest,
	body *scriptexecution.BodyEvidence,
	container *scriptexecution.ContainerEvidence,
) (scriptexecution.CleanupProof, error) {
	*engine.events = append(*engine.events, "engine:cleanup")
	proof := scriptexecution.CleanupProof{
		ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true,
	}
	if container != nil {
		proof.ContainerID = container.ID
		engine.cleanupContainerID = container.ID
	}
	if body != nil {
		proof.BodyDevice, proof.BodyInode, proof.BodyLeaf = body.Device, body.Inode, body.Leaf
	}
	return proof, engine.cleanupErr
}

func (engine *checkpointOrderScriptEngine) Close() error { return nil }

func (engine *checkpointOrderScriptEngine) Quiesce(
	context.Context,
	scriptexecution.QuiescenceRequest,
) (scriptexecution.QuiescenceProof, error) {
	return scriptexecution.QuiescenceProof{}, errors.New(
		"unexpected recovery quiescence during forward Script execution",
	)
}

func bytesOf(value byte, size int) []byte {
	result := make([]byte, size)
	for index := range result {
		result[index] = value
	}
	return result
}
