package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ScriptWorkOwnership excludes body and Entry values. The immutable execution
// binding, not a current Script definition, owns this container projection.
type ScriptWorkOwnership struct {
	OperationID string
	Input       workexecution.Input
	Run         *agentpb.RunScript
	Projection  *agentpb.ScriptRunnerProjection
	Cleanup     *agentpb.ScriptCleanupArtifacts
}

func (repository *ScriptRepository) ResolveReleaseScriptOwnership(ctx context.Context, view workqueue.View, executionID string, reference workexecution.Input) (*ScriptWorkOwnership, error) {
	input, err := repository.loadCapturedReleaseScriptWork(ctx, view, executionID, reference)
	if err != nil {
		return nil, err
	}
	defer input.Execution.Clear()
	return capturedScriptOwnership(input.Execution)
}

func (repository *ScriptRepository) ResolveStandaloneScriptOwnership(ctx context.Context, view workqueue.View) (*ScriptWorkOwnership, error) {
	input, err := repository.LoadScriptWork(ctx, view)
	if err != nil {
		return nil, err
	}
	defer input.Execution.Clear()
	return capturedScriptOwnership(input.Execution)
}

func capturedScriptOwnership(execution scriptexecutions.ScriptExecutionRecord) (*ScriptWorkOwnership, error) {
	executionID := execution.ID
	run, projection := &agentpb.RunScript{}, &agentpb.ScriptRunnerProjection{}
	if proto.Unmarshal(execution.Run, run) != nil || proto.Unmarshal(execution.RunnerProjection, projection) != nil ||
		executionplan.RejectUnknown(run) != nil || executionplan.RejectUnknown(projection) != nil || run.ScriptExecutionId != executionID {
		return nil, errs.New(errs.KindStateConflict, "captured Script ownership is unavailable")
	}
	metadata := &agentpb.ScriptBodyArtifactMetadata{}
	if proto.Unmarshal(execution.BodyMetadata, metadata) != nil || executionplan.RejectUnknown(metadata) != nil || metadata.ScriptExecutionId != executionID {
		return nil, errs.New(errs.KindStateConflict, "captured Script cleanup metadata is unavailable")
	}
	cleanup := &agentpb.ScriptCleanupArtifacts{BodyMetadata: metadata}
	if receipt := execution.BodyPrepared; receipt != nil {
		digest, err := hex.DecodeString(receipt.BodySHA256)
		if err != nil {
			return nil, errs.Wrap(errs.KindStateConflict, err)
		}
		cleanup.BodyPrepared = &agentpb.ScriptBodyPreparedCheckpoint{BodySha256: digest,
			Uid: receipt.UID, Gid: receipt.GID, Device: receipt.Device, Inode: receipt.Inode, Leaf: receipt.Leaf}
	}
	if receipt := execution.ContainerCreated; receipt != nil {
		digest, err := hex.DecodeString(receipt.OwnershipLabelsSHA256)
		if err != nil {
			return nil, errs.Wrap(errs.KindStateConflict, err)
		}
		cleanup.ContainerCreated = &agentpb.ScriptContainerCreatedCheckpoint{
			ContainerId: receipt.ContainerID, OwnershipLabelsSha256: digest}
	}
	return &ScriptWorkOwnership{OperationID: execution.OperationID, Input: execution.Input, Run: run, Projection: projection, Cleanup: cleanup}, nil
}
