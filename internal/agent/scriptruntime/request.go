package scriptruntime

import (
	"bytes"
	"context"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func scriptExecutionRequest(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
) (scriptexecution.Request, *agentpb.ScriptExecutionCheckpoint, error) {
	if ctx == nil || checkpoint == nil || step == nil || step.GetRunScript() == nil || assignment.Plan == nil ||
		assignment.ScriptArtifacts == nil {
		return scriptexecution.Request{}, nil, errs.New(
			errs.KindInternal,
			"agent: Script execution request is incomplete",
		)
	}
	run := step.GetRunScript()
	var projection *agentpb.ScriptRunnerProjection
	var snapshot *agentpb.ResolvedRunnerSnapshot
	for _, candidate := range assignment.Plan.ScriptRunnerProjections {
		if candidate != nil && candidate.SnapshotId == run.RunnerSnapshotId {
			projection = candidate
			break
		}
	}
	for _, candidate := range assignment.Plan.ScriptRunnerSnapshots {
		if candidate != nil && candidate.SnapshotId == run.RunnerSnapshotId {
			snapshot = candidate
			break
		}
	}
	var body *agentpb.ScriptBodyArtifact
	for _, candidate := range assignment.ScriptArtifacts.Bodies {
		if candidate != nil && candidate.Metadata != nil &&
			candidate.Metadata.ScriptExecutionId == run.ScriptExecutionId {
			body = candidate
			break
		}
	}
	if projection == nil || snapshot == nil || body == nil || body.Metadata == nil ||
		projection.SnapshotId != run.RunnerSnapshotId ||
		body.Metadata.ScriptExecutionId != run.ScriptExecutionId || body.Metadata.ScriptId != run.ScriptId ||
		body.Metadata.Generation != run.ScriptGeneration {
		return scriptexecution.Request{}, nil, errs.New(
			errs.KindInternal,
			"agent: Script execution artifacts do not match RunScript",
		)
	}
	var durable *agentpb.ScriptExecutionCheckpoint
	for _, candidate := range assignment.ScriptCheckpoints {
		if candidate != nil && candidate.ScriptExecutionId == run.ScriptExecutionId {
			durable = candidate
			break
		}
	}
	durable, err := executionplan.ValidateScriptExecutionCheckpoint(durable)
	if err != nil {
		return scriptexecution.Request{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	return scriptexecution.Request{
		TaskID: assignment.TaskID, OperationID: assignment.OperationID, AssignmentID: assignment.AssignmentID,
		StepID: step.StepId, ExecutionID: run.ScriptExecutionId,
		PlanHash:     append([]byte(nil), assignment.Plan.PlanHash...),
		Projection:   projection,
		BodyMetadata: proto.Clone(body.Metadata).(*agentpb.ScriptBodyArtifactMetadata),
		Body:         append([]byte(nil), body.Body...),
		Entries: cloneScriptEntriesForSnapshot(
			assignment.ScriptArtifacts.Entries,
			assignment.Plan,
			run.RunnerSnapshotId,
		),
	}, durable, nil
}

func cloneScriptEntriesForSnapshot(
	entries []*agentpb.ScriptEntryArtifact,
	plan *agentpb.ExecutionPlan,
	snapshotID string,
) []*agentpb.ScriptEntryArtifact {
	wanted := make(map[string]struct{})
	for _, snapshot := range plan.ScriptRunnerSnapshots {
		if snapshot == nil || snapshot.SnapshotId != snapshotID {
			continue
		}
		for _, binding := range snapshot.EntryBindings {
			wanted[binding.EntryId+"\x00"+binding.ValueGenerationId] = struct{}{}
		}
	}
	selected := make([]*agentpb.ScriptEntryArtifact, 0, len(wanted))
	for _, entry := range entries {
		if entry == nil || entry.Binding == nil {
			continue
		}
		key := entry.Binding.EntryId + "\x00" + entry.Binding.ValueGenerationId
		if _, exists := wanted[key]; exists {
			selected = append(selected, entry)
		}
	}
	return cloneScriptEntries(selected)
}

func bodyEvidenceFromCheckpoint(value *agentpb.ScriptBodyPreparedCheckpoint) scriptexecution.BodyEvidence {
	return scriptexecution.BodyEvidence{
		SHA256: append([]byte(nil), value.GetBodySha256()...), UID: value.GetUid(), GID: value.GetGid(),
		Device: value.GetDevice(), Inode: value.GetInode(), Leaf: value.GetLeaf(),
	}
}

func containerEvidenceFromCheckpoint(
	value *agentpb.ScriptContainerCreatedCheckpoint,
) scriptexecution.ContainerEvidence {
	return scriptexecution.ContainerEvidence{
		ID: value.GetContainerId(), OwnershipLabelsSHA256: append([]byte(nil), value.GetOwnershipLabelsSha256()...),
	}
}

func sameBodyEvidence(left, right scriptexecution.BodyEvidence) bool {
	return bytes.Equal(left.SHA256, right.SHA256) && left.UID == right.UID && left.GID == right.GID &&
		left.Device == right.Device && left.Inode == right.Inode && left.Leaf == right.Leaf
}

func cloneScriptEntries(values []*agentpb.ScriptEntryArtifact) []*agentpb.ScriptEntryArtifact {
	result := make([]*agentpb.ScriptEntryArtifact, len(values))
	for index, value := range values {
		result[index] = proto.Clone(value).(*agentpb.ScriptEntryArtifact)
	}
	return result
}

func clearScriptExecutionRequest(request *scriptexecution.Request) {
	if request == nil {
		return
	}
	clear(request.Body)
	request.Body = nil
	for _, entry := range request.Entries {
		if entry != nil {
			clear(entry.Value)
			entry.Value = nil
		}
	}
}
