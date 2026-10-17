package attachexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type InspectionState string

const (
	CommandNotCreated     InspectionState = "not_created"
	CommandNotPermitted   InspectionState = "not_permitted"
	CommandExited         InspectionState = "exited"
	CommandRunning        InspectionState = "running"
	CommandUnobservable   InspectionState = "unobservable"
	CommandJournalLost    InspectionState = "journal_unavailable"
	InspectionInterrupted InspectionState = "inspection_interrupted"
	ConsumerUnobservable  InspectionState = "consumer_unobservable"
)

// RemovalInspection accounts for execution, not rollback of command effects.
// Completed preceding subcommands are never described as undone. Only the last
// subcommand can be unresolved because each predecessor must exit successfully
// before the Agent journal permits another exec to receive command input.
type RemovalInspection struct {
	AttemptID        string              `json:"attempt_id"`
	ForwardID        string              `json:"forward_id"`
	Failure          workexecution.Input `json:"failure"`
	CommandOrdinal   uint32              `json:"command_ordinal"`
	PreparedCommands uint32              `json:"prepared_commands"`
	State            InspectionState     `json:"state"`
	ContainerID      string              `json:"container_id,omitempty"`
	ExecID           string              `json:"exec_id,omitempty"`
	ExitCode         *int                `json:"exit_code,omitempty"`
	ObservedAt       time.Time           `json:"observed_at"`
}

func (report RemovalInspection) Validate(input RecoveryInput) error {
	if input.Validate() != nil || input.Attempt.StartedAt == nil && report.State != InspectionInterrupted || report.AttemptID != input.Attempt.ID ||
		report.ForwardID != input.Source.Forward.ID || report.Failure != input.Source.Evidence ||
		report.CommandOrdinal != uint32(input.Source.Failure.Checkpoint.Sequence/2) || report.PreparedCommands > 16 ||
		report.ObservedAt.IsZero() || report.ObservedAt.Location() != time.UTC || report.ObservedAt.Before(input.Attempt.AssignedAt) ||
		input.Attempt.StartedAt != nil && report.ObservedAt.Before(*input.Attempt.StartedAt) {
		return invalid("Attach inspection does not match its captured command")
	}
	switch report.State {
	case CommandNotCreated, CommandJournalLost, InspectionInterrupted, ConsumerUnobservable:
		if report.PreparedCommands != 0 || report.ContainerID != "" || report.ExecID != "" || report.ExitCode != nil {
			return invalid("Attach inspection claims a process without its journal")
		}
		if report.State == ConsumerUnobservable && (!input.Source.Failure.Authority.HasConsumer ||
			input.Source.Failure.Authority.ConsumerOrdinal != report.CommandOrdinal) {
			return invalid("consumer inspection does not identify the interrupted network transition")
		}
	case CommandNotPermitted, CommandExited, CommandRunning, CommandUnobservable:
		if report.PreparedCommands == 0 || len(report.ContainerID) != 64 || len(report.ExecID) != 64 ||
			!ids.ValidContainerID(report.ContainerID) || !ids.ValidContainerID(report.ExecID) ||
			(report.State == CommandExited) != (report.ExitCode != nil) {
			return invalid("Attach inspection process evidence is incomplete")
		}
	default:
		return invalid("Attach inspection state is unsupported")
	}
	return nil
}

func (report RemovalInspection) ExecutionStopped() bool {
	return report.State == CommandNotCreated || report.State == CommandNotPermitted || report.State == CommandExited
}

func EncodeRemovalInspection(input RecoveryInput, report RemovalInspection) ([]byte, workexecution.Input, error) {
	if err := report.Validate(input); err != nil {
		return nil, workexecution.Input{}, err
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return nil, workexecution.Input{}, invalid("Attach inspection cannot be encoded")
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	at, err := ids.Timestamp(ids.KindAttempt, input.Attempt.ID)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	digest := sha256.Sum256(raw)
	reference := workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, at, input.Attempt.ID, "attach-removal/inspection"), SHA256: hex.EncodeToString(digest[:])}
	return raw, reference, nil
}

func DecodeRemovalInspection(input RecoveryInput, raw []byte, reference workexecution.Input) (RemovalInspection, error) {
	if len(raw) == 0 || len(raw) > MaximumRemovalRecoveryBytes {
		return RemovalInspection{}, invalid("Attach inspection is missing or oversized")
	}
	report, err := jcs.Decode[RemovalInspection](raw)
	if err != nil {
		return RemovalInspection{}, invalid("Attach inspection is malformed")
	}
	expected, evidence, err := EncodeRemovalInspection(input, report)
	if err != nil || evidence != reference || !bytes.Equal(expected, raw) {
		return RemovalInspection{}, invalid("Attach inspection differs from its retained evidence")
	}
	return report, nil
}
