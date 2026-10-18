package scriptexecution

import (
	"bytes"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// RecoveryInput cannot carry a shell command, Script body or Entry value. It
// binds cleanup to the failed forward attempt and its captured ownership only.
type RecoveryInput struct {
	Attempt     workexecution.Attempt
	Forward     workexecution.Attempt
	OperationID string
	ExecutionID string
	Projection  *agentpb.ScriptRunnerProjection
	Artifacts   *agentpb.ScriptCleanupArtifacts
}

func (input RecoveryInput) Validate() error {
	forward, recovery := input.Forward, input.Attempt
	if forward.Validate() != nil || recovery.Validate() != nil || forward.Direction != workexecution.Forward ||
		recovery.Direction != workexecution.Recovery || forward.FinishedAt == nil || forward.Result == nil ||
		forward.Outcome == workexecution.Succeeded || recovery.FinishedAt != nil || forward.Number >= recovery.Number ||
		forward.TaskID != recovery.TaskID || forward.WorkItemID != recovery.WorkItemID || forward.Input != recovery.Input ||
		forward.Executor.Executor != workexecution.Agent || recovery.Executor.Executor != workexecution.Agent ||
		forward.Executor.HostID != recovery.Executor.HostID || recovery.AssignedAt.Before(*forward.FinishedAt) ||
		ids.Validate(ids.KindOperation, input.OperationID) != nil || input.ExecutionID == "" ||
		input.Projection == nil || executionplan.RejectUnknown(input.Projection) != nil ||
		input.Artifacts == nil || executionplan.RejectUnknown(input.Artifacts) != nil || input.Artifacts.BodyMetadata == nil {
		return errs.New(errs.KindStateConflict, "Script cleanup input differs from its failed execution")
	}
	metadata := input.Artifacts.BodyMetadata
	if metadata.ScriptExecutionId != input.ExecutionID || ids.Validate(ids.KindScript, metadata.ScriptId) != nil ||
		metadata.Generation == 0 || len(metadata.Sha256) != 32 ||
		input.Projection.Name != "gp-script-"+strings.ToLower(input.ExecutionID) ||
		metadata.Uid != input.Projection.Uid || metadata.Gid != input.Projection.Gid {
		return errs.New(errs.KindStateConflict, "Script cleanup metadata differs from its captured container")
	}
	if body := input.Artifacts.BodyPrepared; body != nil {
		if !bytes.Equal(body.BodySha256, metadata.Sha256) || body.Uid != metadata.Uid || body.Gid != metadata.Gid || body.Device == 0 || body.Inode == 0 || body.Leaf != "body" {
			return errs.New(errs.KindStateConflict, "Script cleanup body receipt differs from its captured metadata")
		}
	}
	return nil
}
