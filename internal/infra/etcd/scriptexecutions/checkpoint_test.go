package scriptexecutions

import (
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// SCRIPT-07/09, TASK-71: a Script's accumulated physical evidence must remain
// bound to its captured input and first authorized native attempt through
// cleanup. This exercises the record owner, not queue CAS or actual containers.
func TestNativeScriptCheckpointsPreserveCapturedAttemptThroughCleanup(t *testing.T) {
	record := nativeCheckpointExecution(t)
	at := record.CreatedAt.Add(time.Second)
	input := ScriptCheckpointInput{TaskID: record.CurrentTaskID, OperationID: record.OperationID,
		WorkItemID: record.WorkItemID, AttemptID: ids.New(ids.KindAttempt), ExecutionID: record.ID,
		Input: record.Input, Executor: workexecution.Identity{Executor: workexecution.Agent,
			HostID: ids.New(ids.KindHost), AgentID: ids.New(ids.KindAgent), Generation: 1}, At: at}
	exit := int32(17)
	steps := []struct {
		state    ScriptExecutionState
		evidence ScriptCheckpointEvidence
	}{
		{ScriptExecutionStartAuthorized, ScriptCheckpointEvidence{Kind: ScriptCheckpointEvidenceStartAuthorized,
			StartAuthorized: &ScriptStartAuthorizedEvidence{}}},
		{ScriptExecutionBodyPrepared, ScriptCheckpointEvidence{Kind: ScriptCheckpointEvidenceBodyPrepared,
			BodyPrepared: &ScriptBodyPreparedEvidence{BodySHA256: record.BodySHA256,
				UID: 65534, GID: 65534, Device: 10, Inode: 20, Leaf: "body"}}},
		{ScriptExecutionContainerCreated, ScriptCheckpointEvidence{Kind: ScriptCheckpointEvidenceContainerCreated,
			ContainerCreated: &ScriptContainerCreatedEvidence{
				ContainerID:           strings.Repeat("a", 64),
				OwnershipLabelsSHA256: strings.Repeat("b", 64),
			}}},
		{ScriptExecutionOutcomeRecorded, ScriptCheckpointEvidence{Kind: ScriptCheckpointEvidenceOutcome,
			Outcome: &ScriptOutcomeEvidence{Reason: ScriptOutcomeNormalExit, ExitCode: &exit, ObservedAt: at}}},
		{ScriptExecutionCleanupProven, ScriptCheckpointEvidence{Kind: ScriptCheckpointEvidenceCleanup,
			Cleanup: &ScriptCleanupEvidence{ContainerID: strings.Repeat("a", 64), BodyDevice: 10, BodyInode: 20,
				BodyLeaf: "body", ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true}}},
	}
	for index, step := range steps {
		input.Sequence, input.StageID = uint64(index+1), ids.New(ids.KindStep)
		input.ExpectedState, input.State, input.Evidence = record.State, step.state, step.evidence
		if index > 0 {
			for _, changed := range []string{"attempt", "input", "host", "process", "generation", "unproved cleanup"} {
				if changed == "unproved cleanup" && step.state != ScriptExecutionCleanupProven {
					continue
				}
				tampered := input
				switch changed {
				case "attempt":
					tampered.AttemptID = ids.New(ids.KindAttempt)
				case "input":
					tampered.Input.SHA256 = strings.Repeat("f", 64)
				case "host":
					tampered.Executor.HostID = ids.New(ids.KindHost)
				case "process":
					tampered.Executor.AgentID = ids.New(ids.KindAgent)
				case "generation":
					tampered.Executor.Generation++
				case "unproved cleanup":
					cleanup := *input.Evidence.Cleanup
					cleanup.ContainerAbsent = false
					tampered.Evidence.Cleanup = &cleanup
				}
				sealNativeCheckpoint(t, &tampered)
				if _, err := AdvanceScriptExecutionRecord(record, tampered); err == nil {
					t.Fatalf("%s accepted changed %s", step.state, changed)
				}
			}
		}
		sealNativeCheckpoint(t, &input)
		next, err := AdvanceScriptExecutionRecord(record, input)
		if err != nil {
			t.Fatalf("advance %s -> %s: %v", record.State, step.state, err)
		}
		if next.Input != record.Input || next.AttemptID != input.AttemptID || next.Executor == nil ||
			*next.Executor != input.Executor || !next.ActiveReference {
			t.Fatal("checkpoint lost captured input/attempt or released retained sources")
		}
		if _, err := AdvanceScriptExecutionRecord(next, input); err == nil {
			// Exact request replay is handled by the durable evidence reader,
			// not by advancing the accumulated record a second time.
			t.Fatal("stale expected state advanced an already-applied checkpoint")
		}
		record = next
	}
	if record.State != ScriptExecutionCleanupProven || record.Cleanup == nil || record.Outcome == nil ||
		record.Outcome.ExitCode == nil || *record.Outcome.ExitCode != 17 || record.ReconciliationRequired {
		t.Fatal("cleanup hid the original nonzero Script exit or lost absence evidence")
	}
}

func sealNativeCheckpoint(t *testing.T, input *ScriptCheckpointInput) {
	t.Helper()
	raw, digest, err := EncodeCheckpointEvidence(*input)
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	input.PayloadSHA256 = digest
}

func nativeCheckpointExecution(t *testing.T) ScriptExecutionRecord {
	t.Helper()
	executionID, snapshotID := ids.NewULID(), ids.NewULID()
	environmentID, serviceID, releaseID := ids.New(
		ids.KindEnvironment,
	), ids.New(
		ids.KindService,
	), ids.New(
		ids.KindDeployment,
	)
	scriptID := ids.New(ids.KindScript)
	image := "sha256:" + strings.Repeat("c", 64)
	bodyDigest, serviceDigest := sha256.Sum256([]byte("exit 17")), sha256.Sum256([]byte("captured service"))
	projection := &agentpb.ScriptRunnerProjection{SnapshotId: snapshotID, Image: image, Uid: 65534, Gid: 65534}
	marshal := func(value proto.Message) ([]byte, [sha256.Size]byte) {
		t.Helper()
		raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw, sha256.Sum256(raw)
	}
	projectionBytes, projectionDigest := marshal(projection)
	defer clear(projectionBytes)
	snapshot := &agentpb.ResolvedRunnerSnapshot{SnapshotId: snapshotID, ScriptExecutionId: executionID,
		EnvironmentId: environmentID, ServiceId: serviceID, ReleaseId: releaseID, RenderGeneration: 1,
		LocalImageId: image, ServiceDefinitionSha256: serviceDigest[:], RunnerProjectionSha256: projectionDigest[:]}
	snapshotBytes, snapshotDigest := marshal(snapshot)
	defer clear(snapshotBytes)
	run := &agentpb.RunScript{ScriptExecutionId: executionID, RunnerSnapshotId: snapshotID,
		ScriptId: scriptID, ScriptGeneration: 1, EnvironmentId: environmentID, ServiceId: serviceID,
		ReleaseId: releaseID, RenderGeneration: 1, BodySha256: bodyDigest[:],
		ServiceDefinitionSha256: serviceDigest[:], RunnerSnapshotSha256: snapshotDigest[:]}
	body := &agentpb.ScriptBodyArtifactMetadata{ScriptExecutionId: executionID, ScriptId: scriptID,
		Generation: 1, Sha256: bodyDigest[:], Uid: 65534, Gid: 65534}
	record, err := NewExecution(ExecutionInput{TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
		OperationID: ids.New(ids.KindOperation), PlanID: ids.New(ids.KindPlan), ScriptSetGeneration: "captured-set",
		Snapshot: snapshot, Projection: projection, Run: run, Body: body}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(record.Clear)
	return record
}
