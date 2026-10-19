package agent

import (
	"context"
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (exchange *nativeExchange) PrepareConfigRecovery(ctx context.Context,
	assignment *agentpb.BackupRecoveryAssignment, ordinal uint32,
) (*agentpb.BackupConfigRecoveryPrepareAck, error) {
	exchange.mu.Lock()
	identity := exchange.identity
	exchange.mu.Unlock()
	owned, _, _, err := executionplan.OpenBackupRecoveryAssignment(assignment, identity)
	if err != nil {
		return nil, err
	}
	if uint64(ordinal) >= uint64(len(owned.Input.Steps)) || owned.Input.Steps[ordinal].GetRestore().GetConfig() == nil {
		return nil, errs.New(errs.KindStateConflict, "Config recovery source is unavailable")
	}
	response, err := exchange.request(ctx, &agentpb.AgentMessage{
		Payload: &agentpb.AgentMessage_BackupConfigRecoveryPrepareRequest{
			BackupConfigRecoveryPrepareRequest: &agentpb.BackupConfigRecoveryPrepareRequest{
				Attempt:       owned.Attempt,
				SourceOrdinal: ordinal,
			},
		},
	}, owned.Attempt, "backup_config_recovery_prepare", fmt.Sprint(ordinal))
	if err != nil {
		return nil, err
	}
	ack := response.GetBackupConfigRecoveryPrepareAck()
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.SourceOrdinal != ordinal {
		return nil, errs.New(errs.KindStateConflict, "Config recovery publication acknowledgement is invalid")
	}
	prepared, _, _, err := executionplan.OpenBackupRecoveryAssignment(ack.Assignment, identity)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(prepared.Attempt, owned.Attempt) || !proto.Equal(prepared.Input, owned.Input) ||
		string(prepared.RecoverySource) != string(owned.RecoverySource) || prepared.RestoreMode != owned.RestoreMode {
		return nil, errs.New(errs.KindStateConflict, "Config recovery publication changed captured authority")
	}
	config := prepared.Input.Steps[ordinal].GetRestore().GetConfig()
	if ack.Completed == nil || ack.Completed.RestoreGenerationId != config.RestoreGenerationId ||
		!proto.Equal(ack.Completed.Content, config.GetExpectedArchive().GetContent()) {
		return nil, errs.New(errs.KindStateConflict, "Config recovery receipt differs from the captured generation")
	}
	return proto.CloneOf(ack), nil
}
