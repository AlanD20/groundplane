package agentworkjournal

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Assignment contains metadata and receipts only. It cannot retain Script body
// or Entry values. A proof is durable before it is submitted for settlement.
type ScriptRecoveryRecord struct {
	Assignment []byte                         `json:"assignment"`
	Proof      *scriptexecution.RecoveryProof `json:"proof,omitempty"`
	Receipt    *workexecution.Attempt         `json:"receipt,omitempty"`
}

func (record ScriptRecoveryRecord) input(attempt workexecution.Attempt) (scriptexecution.RecoveryInput, error) {
	if len(record.Assignment) == 0 || len(record.Assignment) > 64<<10 {
		return scriptexecution.RecoveryInput{}, invalid()
	}
	message := &agentpb.ScriptRecoveryAssignment{}
	if err := proto.Unmarshal(record.Assignment, message); err != nil {
		return scriptexecution.RecoveryInput{}, invalid()
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil || !bytes.Equal(canonical, record.Assignment) {
		return scriptexecution.RecoveryInput{}, invalid()
	}
	input, err := scriptexecution.DecodeRecoveryAssignment(message, attempt.Executor)
	if err != nil {
		return scriptexecution.RecoveryInput{}, err
	}
	if !workexecution.SameAssignment(input.Attempt, attempt) || attempt.StartedAt == nil {
		return scriptexecution.RecoveryInput{}, conflict()
	}
	input.Attempt = attempt
	return input, nil
}

func (record attemptRecord) validateScriptRecovery() error {
	recovery := record.ScriptRecovery
	if recovery == nil || record.Recovery != nil || record.Release != nil || record.Failure != nil || record.Effect != nil ||
		record.Completion != nil || record.ScriptCompletion != nil || len(record.Script) != 0 || record.Sequence != 0 {
		return invalid()
	}
	input, err := recovery.input(record.Attempt)
	if err != nil {
		return err
	}
	if recovery.Proof == nil {
		if recovery.Receipt != nil || !record.UpdatedAt.Equal(*record.Attempt.StartedAt) {
			return invalid()
		}
		return nil
	}
	_, evidence, err := scriptexecution.EncodeRecoveryProof(input, *recovery.Proof)
	if err != nil {
		return err
	}
	if !record.UpdatedAt.Equal(recovery.Proof.ObservedAt) {
		return invalid()
	}
	if receipt := recovery.Receipt; receipt != nil {
		if receipt.Validate() != nil || !workexecution.SameAssignment(record.Attempt, *receipt) || receipt.FinishedAt == nil ||
			receipt.Result == nil || *receipt.Result != evidence || receipt.Outcome != workexecution.Succeeded || receipt.FinishedAt.Before(record.UpdatedAt) {
			return invalid()
		}
	}
	return nil
}
