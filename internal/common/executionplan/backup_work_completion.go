package executionplan

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func DecodeBackupWorkCompletion(attempt workexecution.Attempt, evidence workexecution.Input,
	ack *agentpb.BackupWorkCompletionAck,
) (workexecution.Attempt, error) {
	wire, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if attempt.StartedAt == nil || evidence.Validate() != nil || ack == nil || RejectUnknown(ack) != nil ||
		!proto.Equal(wire, ack.Attempt) || ack.Evidence == nil || ack.Evidence.InputId != evidence.ID ||
		hex.EncodeToString(ack.Evidence.Sha256) != evidence.SHA256 || ack.StartedAt == nil || ack.FinishedAt == nil ||
		ack.StartedAt.CheckValid() != nil || ack.FinishedAt.CheckValid() != nil ||
		!ack.StartedAt.AsTime().Equal(*attempt.StartedAt) {
		return workexecution.Attempt{}, errs.New(
			errs.KindStateConflict,
			"Backup completion does not acknowledge this started attempt and checkpoint",
		)
	}
	outcome := workexecution.Outcome(ack.Outcome)
	if outcome != workexecution.Succeeded && outcome != workexecution.Cancelled && outcome != workexecution.Superseded {
		return workexecution.Attempt{}, errs.New(errs.KindStateConflict, "Backup completion has an unexpected outcome")
	}
	finished := ack.FinishedAt.AsTime().UTC()
	terminal := attempt
	terminal.FinishedAt, terminal.Outcome, terminal.Result = &finished, outcome, &evidence
	if err := terminal.Validate(); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
