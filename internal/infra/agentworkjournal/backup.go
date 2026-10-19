package agentworkjournal

import (
	"bytes"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BackupRecord retains only the latest report per captured source, including an
// unsent command result. It never stores credentials or a diagnostic event log.
type BackupRecord struct {
	Recovery      *BackupRecoveryRecord             `json:"recovery,omitempty"`
	Sources       []BackupSourceReport              `json:"sources"`
	Completion    *workexecution.Attempt            `json:"completion,omitempty"`
	FailureReason executionplan.BackupFailureReason `json:"failure_reason,omitempty"`
	FailureAck    []byte                            `json:"failure_ack,omitempty"`
}

type BackupSourceReport struct {
	Request         []byte `json:"request,omitempty"`
	Acknowledgement []byte `json:"acknowledgement,omitempty"`
}

func (source BackupSourceReport) Report() (*agentpb.BackupWorkCheckpointRequest, error) {
	if len(source.Request) == 0 {
		return nil, nil
	}
	request := &agentpb.BackupWorkCheckpointRequest{}
	if proto.Unmarshal(source.Request, request) != nil || executionplan.RejectUnknown(request) != nil {
		return nil, invalid()
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil || !bytes.Equal(raw, source.Request) {
		return nil, invalid()
	}
	return request, nil
}

func (source BackupSourceReport) Ack() (*agentpb.BackupWorkCheckpointAck, error) {
	if len(source.Acknowledgement) == 0 {
		return nil, nil
	}
	ack := &agentpb.BackupWorkCheckpointAck{}
	if proto.Unmarshal(source.Acknowledgement, ack) != nil || executionplan.RejectUnknown(ack) != nil {
		return nil, invalid()
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(ack)
	if err != nil || !bytes.Equal(raw, source.Acknowledgement) {
		return nil, invalid()
	}
	return ack, nil
}

func (record attemptRecord) validateBackup() error {
	if record.RemovalRecovery != nil || record.LifecycleRecovery != nil || record.Lifecycle != nil ||
		record.AttachProvisionRecovery != nil || record.AttachProvisionFailure != nil || record.AttachProvision != nil ||
		record.EntryRecovery != nil || record.EntryConfiguration != nil || record.ComponentRecovery != nil || record.Component != nil ||
		record.NetworkRecovery != nil || record.Network != nil || record.VolumeRecovery != nil || record.Volume != nil ||
		record.AttachRecovery != nil || record.AttachFailure != nil || record.AttachRemoval != nil || record.EnvironmentFailure != nil ||
		record.EnvironmentRemoval != nil || record.ScriptFailure != nil || record.ScriptRecovery != nil || len(record.Script) != 0 ||
		record.Release != nil || record.Effect != nil || record.Completion != nil || record.Failure != nil || record.Recovery != nil || record.ScriptCompletion != nil {
		return invalid()
	}
	if record.Backup.Recovery != nil {
		return record.validateBackupRecovery()
	}
	if record.Attempt.Validate() != nil || record.Attempt.Executor.Executor != workexecution.Agent ||
		record.Attempt.Direction != workexecution.Forward || record.Attempt.StartedAt == nil || record.Attempt.FinishedAt != nil ||
		record.Sequence == 0 || record.UpdatedAt.Before(*record.Attempt.StartedAt) || len(record.Backup.Sources) == 0 {
		return invalid()
	}
	// The initial durable start consumes the first local sequence.
	sequences := uint64(1)
	pending := 0
	for ordinal, source := range record.Backup.Sources {
		request, err := source.Report()
		if err != nil {
			return err
		}
		if request == nil {
			if len(source.Acknowledgement) != 0 {
				return invalid()
			}
			continue
		}
		attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, record.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(attempt, record.Attempt) ||
			request.SourceOrdinal != uint32(ordinal) ||
			request.CheckpointSequence == 0 ||
			sequences > ^uint64(0)-request.CheckpointSequence {
			return invalid()
		}
		sequences += request.CheckpointSequence
		digest, err := executionplan.BackupWorkCheckpointDigest(request)
		if err != nil {
			return err
		}
		ack, err := source.Ack()
		if err != nil {
			return err
		}
		if ack == nil {
			pending++
			continue
		}
		if !proto.Equal(ack.Attempt, request.Attempt) || ack.SourceOrdinal != request.SourceOrdinal ||
			ack.CheckpointSequence != request.CheckpointSequence || !bytes.Equal(ack.RequestSha256, digest) ||
			ack.Committed == nil || ack.Committed.DedupeKeyModRevision <= request.GetPrecedingCheckpoint().GetDedupeKeyModRevision() ||
			len(ack.Committed.AuthorityDigest) != 32 {
			return invalid()
		}
	}
	if record.Backup.FailureReason != "" {
		if !record.Backup.FailureReason.Valid() || pending != 0 || sequences == ^uint64(0) {
			return invalid()
		}
		sequences++
	} else if len(record.Backup.FailureAck) != 0 {
		return invalid()
	}
	if pending > 1 || sequences < record.Sequence {
		return invalid()
	}
	if terminal := record.Backup.Completion; terminal != nil {
		if record.Backup.FailureReason != "" {
			ack := &agentpb.BackupWorkFailureAck{}
			if proto.Unmarshal(record.Backup.FailureAck, ack) != nil {
				return invalid()
			}
			saved, err := executionplan.DecodeBackupWorkFailureAck(record.Attempt, record.Backup.FailureReason, ack)
			if err != nil || terminal.Validate() != nil || !workexecution.SameAssignment(saved, *terminal) ||
				terminal.StartedAt == nil || !terminal.StartedAt.Equal(*saved.StartedAt) || terminal.FinishedAt == nil ||
				!terminal.FinishedAt.Equal(*saved.FinishedAt) || terminal.FinishedAt.Before(record.UpdatedAt) ||
				terminal.Outcome != saved.Outcome || terminal.Result == nil || *terminal.Result != *saved.Result {
				return invalid()
			}
			return nil
		}
		evidence, err := record.Backup.CompletionEvidence(record.Attempt.Input.ID)
		if err != nil || pending != 0 || terminal.Validate() != nil ||
			!workexecution.SameAssignment(*terminal, record.Attempt) ||
			terminal.StartedAt == nil ||
			!terminal.StartedAt.Equal(*record.Attempt.StartedAt) ||
			terminal.FinishedAt == nil ||
			terminal.FinishedAt.Before(record.UpdatedAt) ||
			terminal.Result == nil ||
			*terminal.Result != evidence ||
			(terminal.Outcome != workexecution.Succeeded && terminal.Outcome != workexecution.Cancelled && terminal.Outcome != workexecution.Superseded) {
			return invalid()
		}
	}
	if len(record.Backup.FailureAck) != 0 {
		return invalid()
	}
	return nil
}

// CompletionEvidence selects the last committed native checkpoint, not the
// last source in declaration order. Missing acknowledgements forbid completion.
func (record *BackupRecord) CompletionEvidence(inputID string) (workexecution.Input, error) {
	var latest *agentpb.BackupWorkCheckpointAck
	for _, source := range record.Sources {
		if len(source.Request) == 0 {
			continue
		}
		ack, err := source.Ack()
		if err != nil || ack == nil || ack.Committed == nil {
			return workexecution.Input{}, conflict()
		}
		if latest == nil || ack.Committed.DedupeKeyModRevision > latest.Committed.DedupeKeyModRevision {
			latest = ack
		}
	}
	if latest == nil {
		return workexecution.Input{}, conflict()
	}
	evidence := workexecution.Input{ID: inputID, SHA256: hex.EncodeToString(latest.RequestSha256)}
	return evidence, evidence.Validate()
}
