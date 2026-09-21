package agent

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func capturedContainerFailureFixture() (testtaskassignment.Assignment, *agentpb.ExecutionStep, [sha256.Size]byte) {
	body := []byte("echo migration\n")
	bodyDigest := sha256.Sum256(body)
	planDigest := sha256.Sum256([]byte("sealed captured-container failure plan"))
	executionID, snapshotID := ids.NewULID(), ids.NewULID()
	scriptID, stepID := ids.New(ids.KindScript), ids.New(ids.KindStep)
	return testtaskassignment.Assignment{
			AssignmentID: ids.New(ids.KindAssignment), TaskID: ids.New(ids.KindTask),
			OperationID: ids.New(ids.KindOperation), Deadline: time.Now().Add(time.Minute),
			Plan: &agentpb.ExecutionPlan{
				PlanHash:                append([]byte(nil), planDigest[:]...),
				ScriptRunnerProjections: []*agentpb.ScriptRunnerProjection{{SnapshotId: snapshotID}},
				ScriptRunnerSnapshots:   []*agentpb.ResolvedRunnerSnapshot{{SnapshotId: snapshotID}},
			},
			ScriptArtifacts: &agentpb.ScriptAssignmentArtifacts{Bodies: []*agentpb.ScriptBodyArtifact{{
				Metadata: &agentpb.ScriptBodyArtifactMetadata{
					ScriptExecutionId: executionID, ScriptId: scriptID, Generation: 1,
					Size: uint32(len(body)), Sha256: append([]byte(nil), bodyDigest[:]...),
				},
				Body: append([]byte(nil), body...),
			}}},
			ScriptCheckpoints: []*agentpb.ScriptExecutionCheckpoint{{
				ScriptExecutionId: executionID,
				State:             agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED,
			}},
		}, &agentpb.ExecutionStep{StepId: stepID, Payload: &agentpb.ExecutionStep_RunScript{
			RunScript: &agentpb.RunScript{
				ScriptExecutionId: executionID, ScriptId: scriptID, ScriptGeneration: 1,
				RunnerSnapshotId: snapshotID,
			},
		}}, bodyDigest
}

func setExplicitScriptFixture(t *testing.T, assignment *testtaskassignment.Assignment) {
	t.Helper()
	snapshot, projection := assignment.Plan.ScriptRunnerSnapshots[0], assignment.Plan.ScriptRunnerProjections[0]
	executionContext := &agentpb.ScriptExplicitExecutionContext{
		ImageReference: "example.test/setup@sha256:" + strings.Repeat("c", 64), User: "1000:1000",
		Volumes: []*agentpb.ScriptExplicitVolumeGrant{{
			VolumeId: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", Target: "/etc/tls", ReadOnly: false,
		}},
		EntryIds: []string{"ev_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	}
	digest, err := executionplan.ScriptExecutionContextDigest(executionContext)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.ExplicitExecution = &agentpb.ScriptExplicitExecutionAuthority{
		Context: executionContext, ContextSha256: digest, ScriptModRevision: 10,
		ReleaseLocalImageId: "sha256:" + strings.Repeat("b", 64),
	}
	snapshot.LocalImageId = "sha256:" + strings.Repeat("c", 64)
	projection.Image, projection.WorkingDir, projection.Uid, projection.Gid = snapshot.LocalImageId, "/", 1000, 1000
	snapshot.Mounts = []*agentpb.ScriptRunnerMount{{
		SourceId: executionContext.Volumes[0].VolumeId,
		RenderedMount: &agentpb.ScriptMount{
			Type: "volume", Source: "gp_vol_" + executionContext.Volumes[0].VolumeId,
			Target: "/etc/tls", ReadOnly: false, VolumeNoCopy: true,
		},
	}}
	projection.Mounts = snapshot.Mounts
	snapshot.EntryBindings = []*agentpb.ScriptRunnerEntryBinding{{
		EntryId: executionContext.EntryIds[0], ValueGenerationId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Kind: agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV, EnvironmentKey: "SETUP_VALUE",
	}}
	projection.EntryBindings = snapshot.EntryBindings
	assignment.ScriptArtifacts.Entries = []*agentpb.ScriptEntryArtifact{
		{Binding: snapshot.EntryBindings[0], Value: []byte("declared-fixture-value")},
		{Binding: &agentpb.ScriptRunnerEntryBinding{
			EntryId: "ev_01ARZ3NDEKTSV4RRFFQ69G5FAW", ValueGenerationId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			Kind: agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV, EnvironmentKey: "AMBIENT_VALUE",
		}, Value: []byte("unselected-fixture-value")},
	}
}

type checkpointOrderScriptEngine struct {
	createdImage string
	events       *[]string
	bodyDigest   [sha256.Size]byte
	runErr       error
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
	return scriptexecution.ContainerEvidence{
		ID: strings.Repeat("a", 64), OwnershipLabelsSHA256: repeatedScriptByte(3, sha256.Size),
	}, nil
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
	_ scriptexecution.Request,
	body *scriptexecution.BodyEvidence,
	container *scriptexecution.ContainerEvidence,
) (scriptexecution.CleanupProof, error) {
	*engine.events = append(*engine.events, "engine:cleanup")
	proof := scriptexecution.CleanupProof{
		ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true,
	}
	if container != nil {
		proof.ContainerID = container.ID
	}
	if body != nil {
		proof.BodyDevice, proof.BodyInode, proof.BodyLeaf = body.Device, body.Inode, body.Leaf
	}
	return proof, nil
}

func (*checkpointOrderScriptEngine) Close() error { return nil }

type capturedProjectionScriptEngine struct {
	*checkpointOrderScriptEngine
	projection *agentpb.ScriptRunnerProjection
	entries    []*agentpb.ScriptEntryArtifact
}

func (engine *capturedProjectionScriptEngine) CreateContainer(
	ctx context.Context,
	request scriptexecution.Request,
	body scriptexecution.BodyEvidence,
) (scriptexecution.ContainerEvidence, error) {
	engine.projection = proto.Clone(request.Projection).(*agentpb.ScriptRunnerProjection)
	engine.entries = make([]*agentpb.ScriptEntryArtifact, len(request.Entries))
	for index, entry := range request.Entries {
		engine.entries[index] = proto.Clone(entry).(*agentpb.ScriptEntryArtifact)
	}
	return engine.checkpointOrderScriptEngine.CreateContainer(ctx, request, body)
}

func assertExplicitScriptCapture(
	t *testing.T,
	assignment testtaskassignment.Assignment,
	engine *capturedProjectionScriptEngine,
) {
	t.Helper()
	if !proto.Equal(engine.projection, assignment.Plan.ScriptRunnerProjections[0]) {
		t.Fatal("container creation did not receive the exact explicit runner projection")
	}
	if engine.createdImage == assignment.Plan.ScriptRunnerSnapshots[0].ExplicitExecution.ReleaseLocalImageId {
		t.Fatal("setup container used the consumer Release image")
	}
	if len(engine.entries) != 1 ||
		!proto.Equal(engine.entries[0].Binding, assignment.Plan.ScriptRunnerSnapshots[0].EntryBindings[0]) ||
		string(engine.entries[0].Value) != "declared-fixture-value" {
		t.Fatal("container creation did not receive only its declared Entry generation")
	}
	for _, entry := range assignment.ScriptArtifacts.Entries {
		if len(entry.Value) != 0 {
			t.Fatal("completed assignment retained an Entry value")
		}
	}
}

func repeatedScriptByte(value byte, size int) []byte {
	result := make([]byte, size)
	for index := range result {
		result[index] = value
	}
	return result
}
