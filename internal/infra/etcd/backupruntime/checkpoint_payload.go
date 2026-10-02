package backupruntime

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BackupCheckpointDigest binds the complete typed delivery, not merely its
// artifact fields. A replay with changed execution or predecessor is different.
func BackupCheckpointDigest(input BackupCheckpointInput) (string, error) {
	request, err := backupCheckpointRequest(input)
	if err != nil {
		return "", err
	}
	digest, err := executionplan.ComputeBackupCheckpointPayloadDigest(request)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest), nil
}

func ValidateBackupCheckpointInput(input BackupCheckpointInput) error {
	if input.AgentID == "" {
		if input.AgentGeneration != 0 {
			return errs.New(errs.KindValidationFailed, "controller checkpoint agent identity is invalid")
		}
	} else if recordcodec.ValidateID(ids.KindAgent, input.AgentID) != nil || input.AgentGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "agent checkpoint identity is invalid")
	}
	if input.AssignmentGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "backup assignment generation is invalid")
	}
	request, err := backupCheckpointRequest(input)
	if err != nil {
		return err
	}
	_, err = executionplan.ValidateBackupCheckpointRequest(request, input.Sequence)
	return err
}

func backupCheckpointRequest(input BackupCheckpointInput) (*agentpb.BackupCheckpointRequest, error) {
	if input.Request == nil || input.Request.TaskId != input.TaskID ||
		input.Request.AssignmentId != input.AssignmentID || input.Request.StepId != input.StepID ||
		input.Request.ExecutionId != input.ExecutionID || input.Request.CheckpointSequence != input.Sequence ||
		hex.EncodeToString(input.Request.AuthorityDigest) != input.AuthoritySHA256 ||
		input.Request.GetPrecedingCheckpoint().GetDedupeKeyModRevision() != input.PrecedingCheckpointRevision {
		return nil, errs.New(errs.KindValidationFailed, "backup checkpoint delivery projection is inconsistent")
	}
	return input.Request, nil
}
