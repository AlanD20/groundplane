package agentchannel

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ScriptCheckpointer interface {
	CheckpointScript(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.ScriptCheckpointRequest,
	) (*agentpb.ScriptCheckpointAck, error)
}
