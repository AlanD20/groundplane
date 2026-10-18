package scriptexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func EncodeRecoveryCompletion(input RecoveryInput, proof RecoveryProof) (*agentpb.ScriptRecoveryCompletionRequest, error) {
	raw, evidence, err := EncodeRecoveryProof(input, proof)
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(evidence.SHA256)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return &agentpb.ScriptRecoveryCompletionRequest{Attempt: attempt, Proof: raw,
		Evidence: &agentpb.WorkInputReference{InputId: evidence.ID, Sha256: digest}}, nil
}

func DecodeRecoveryCompletion(input RecoveryInput, proof RecoveryProof, ack *agentpb.ScriptRecoveryCompletionAck) (workexecution.Attempt, error) {
	request, err := EncodeRecoveryCompletion(input, proof)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if ack == nil || executionplan.RejectUnknown(ack) != nil || !proto.Equal(ack.Attempt, request.Attempt) ||
		!proto.Equal(ack.Evidence, request.Evidence) || ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil || ack.FinishedAt.AsTime().Before(proof.ObservedAt) {
		return workexecution.Attempt{}, errs.New(errs.KindStateConflict, "Script cleanup acknowledgement differs from its submitted proof")
	}
	terminal := input.Attempt
	finished := ack.FinishedAt.AsTime().UTC()
	terminal.FinishedAt, terminal.Outcome = &finished, workexecution.Succeeded
	terminal.Result = &workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	if err := terminal.Validate(); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
