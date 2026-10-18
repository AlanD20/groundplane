package scriptruntime

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Invocation is one captured Script operation within native work. Release hooks
// carry their own Script input while retaining the parent work attempt.
type Invocation struct {
	Attempt     workexecution.Attempt
	OperationID string
	Input       workexecution.Input
	Run         *agentpb.RunScript
	Snapshot    *agentpb.ResolvedRunnerSnapshot
	Projection  *agentpb.ScriptRunnerProjection
	Body        *agentpb.ScriptBodyArtifact
	Entries     []*agentpb.ScriptEntryArtifact
	Checkpoint  *agentpb.ScriptExecutionCheckpoint
}

func scriptExecutionRequest(
	ctx context.Context,
	invocation Invocation,
	checkpoint func(context.Context, *agentpb.ScriptCheckpointRequest) error,
) (scriptexecution.Request, *agentpb.ScriptExecutionCheckpoint, error) {
	if ctx == nil || checkpoint == nil || invocation.Attempt.Validate() != nil ||
		invocation.Attempt.Executor.Executor != workexecution.Agent || invocation.Attempt.FinishedAt != nil ||
		ids.Validate(ids.KindOperation, invocation.OperationID) != nil || invocation.Input.Validate() != nil {
		return scriptexecution.Request{}, nil, errs.New(errs.KindValidationFailed, "agent: Script work identity is invalid")
	}
	run, snapshot, projection, body := invocation.Run, invocation.Snapshot, invocation.Projection, invocation.Body
	if run == nil || snapshot == nil || projection == nil || body == nil || body.Metadata == nil ||
		snapshot.SnapshotId != run.RunnerSnapshotId || projection.SnapshotId != run.RunnerSnapshotId ||
		body.Metadata.ScriptExecutionId != run.ScriptExecutionId || body.Metadata.ScriptId != run.ScriptId ||
		body.Metadata.Generation != run.ScriptGeneration {
		return scriptexecution.Request{}, nil, errs.New(errs.KindValidationFailed, "agent: captured Script artifacts do not match execution")
	}
	durable, err := executionplan.ValidateScriptExecutionCheckpoint(invocation.Checkpoint)
	if err != nil {
		return scriptexecution.Request{}, nil, err
	}
	if durable.ScriptExecutionId != run.ScriptExecutionId {
		return scriptexecution.Request{}, nil, errs.New(errs.KindValidationFailed, "agent: Script checkpoint belongs to another execution")
	}
	entries, err := cloneScriptEntriesForSnapshot(invocation.Entries, snapshot)
	if err != nil {
		return scriptexecution.Request{}, nil, err
	}
	return scriptexecution.Request{
		TaskID: invocation.Attempt.TaskID, WorkItemID: invocation.Attempt.WorkItemID,
		AttemptID: invocation.Attempt.ID, AssignedAt: invocation.Attempt.AssignedAt, OperationID: invocation.OperationID,
		ExecutionID: run.ScriptExecutionId, Input: invocation.Input,
		Projection:   proto.Clone(projection).(*agentpb.ScriptRunnerProjection),
		BodyMetadata: proto.Clone(body.Metadata).(*agentpb.ScriptBodyArtifactMetadata),
		Body:         append([]byte(nil), body.Body...), Entries: entries,
	}, durable, nil
}

func cloneScriptEntriesForSnapshot(
	entries []*agentpb.ScriptEntryArtifact,
	snapshot *agentpb.ResolvedRunnerSnapshot,
) ([]*agentpb.ScriptEntryArtifact, error) {
	wanted := make(map[string]*agentpb.ScriptRunnerEntryBinding, len(snapshot.EntryBindings))
	for _, binding := range snapshot.EntryBindings {
		if binding == nil {
			return nil, errs.New(errs.KindValidationFailed, "agent: Script entry binding is missing")
		}
		key := binding.EntryId + "\x00" + binding.ValueGenerationId
		if _, duplicate := wanted[key]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "agent: captured Script entry binding is duplicated")
		}
		wanted[key] = binding
	}
	selected := make([]*agentpb.ScriptEntryArtifact, 0, len(wanted))
	seen := make(map[string]struct{}, len(wanted))
	for _, entry := range entries {
		if entry == nil || entry.Binding == nil {
			return nil, errs.New(errs.KindValidationFailed, "agent: Script entry artifact is missing")
		}
		key := entry.Binding.EntryId + "\x00" + entry.Binding.ValueGenerationId
		binding, needed := wanted[key]
		if !needed {
			continue
		}
		if !proto.Equal(binding, entry.Binding) {
			return nil, errs.New(errs.KindValidationFailed, "agent: Script entry binding differs from captured input")
		}
		if _, duplicate := seen[key]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "agent: Script entry artifact is duplicated")
		}
		seen[key] = struct{}{}
		selected = append(selected, entry)
	}
	if len(seen) != len(wanted) {
		return nil, errs.New(errs.KindValidationFailed, "agent: captured Script entry values are incomplete")
	}
	return cloneScriptEntries(selected), nil
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
