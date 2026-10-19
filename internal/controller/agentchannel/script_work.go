package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (server *Server) checkpointScriptWork(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.ScriptWorkCheckpointRequest,
) (*agentpb.ScriptWorkCheckpointAck, error) {
	if server == nil || server.scriptCheckpoints == nil || request == nil ||
		executionplan.RejectUnknown(request) != nil ||
		request.Checkpoint == nil {
		return nil, errs.New(errs.KindValidationFailed, "native Script checkpoint request is invalid")
	}
	attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	checkpoint := request.Checkpoint
	if checkpoint.TaskId != attempt.TaskID || checkpoint.WorkItemId != attempt.WorkItemID ||
		checkpoint.AttemptId != attempt.ID {
		return nil, errs.New(errs.KindStateConflict, "Script checkpoint differs from its native attempt")
	}
	ack, err := server.scriptCheckpoints.CheckpointScript(ctx, authorization, checkpoint)
	if err != nil {
		return nil, err
	}
	return &agentpb.ScriptWorkCheckpointAck{Attempt: proto.CloneOf(request.Attempt), Checkpoint: ack}, nil
}
