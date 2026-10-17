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
	ConsumerInspected     InspectionState = "consumer_inspected"
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
	Consumer         *ConsumerInspection `json:"consumer,omitempty"`
}

// ConsumerInspection reports endpoint state after the captured forward helper
// has stopped. CandidateSHA256 identifies retained intent; Detached distinguishes
// completed and incomplete network effects.
type ConsumerInspection struct {
	NetworkID       string `json:"network_id"`
	ServiceID       string `json:"service_id"`
	CandidateSHA256 string `json:"candidate_sha256"`
	HelperStopped   bool   `json:"helper_stopped"`
	Detached        bool   `json:"detached"`
}

func (report RemovalInspection) Validate(input RecoveryInput) error {
	if input.Validate() != nil || input.Attempt.StartedAt == nil && report.State != InspectionInterrupted || report.AttemptID != input.Attempt.ID ||
		report.ForwardID != input.Source.Forward.ID || report.Failure != input.Source.Evidence ||
		report.CommandOrdinal != uint32(input.Source.Failure.Checkpoint.Sequence/2) || report.PreparedCommands > 16 ||
		report.ObservedAt.IsZero() || report.ObservedAt.Location() != time.UTC || report.ObservedAt.Before(input.Attempt.AssignedAt) ||
		input.Attempt.StartedAt != nil && report.ObservedAt.Before(*input.Attempt.StartedAt) {
		return invalid("Attach inspection does not match its captured command")
	}
	if input.RequiresConsumer() && report.State != ConsumerInspected &&
		report.State != InspectionInterrupted && report.State != CommandJournalLost {
		return invalid("consumer recovery requires endpoint inspection, not backing exec evidence")
	}
	switch report.State {
	case ConsumerInspected:
		authority := input.Source.Failure.Authority
		if !input.RequiresConsumer() || report.Consumer == nil || !report.Consumer.HelperStopped ||
			report.Consumer.NetworkID != authority.ConsumerNetworkID || report.Consumer.ServiceID != authority.ConsumerServiceID ||
			report.Consumer.CandidateSHA256 != authority.ConsumerCandidateSHA256 ||
			report.PreparedCommands != 0 || report.ContainerID != "" || report.ExecID != "" || report.ExitCode != nil {
			return invalid("Detach consumer inspection does not prove its captured helper and endpoint state")
		}
	case CommandNotCreated, CommandJournalLost, InspectionInterrupted:
		if report.PreparedCommands != 0 || report.ContainerID != "" || report.ExecID != "" || report.ExitCode != nil {
			return invalid("Attach inspection claims a process without its journal")
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
	if report.State != ConsumerInspected && report.Consumer != nil {
		return invalid("non-consumer inspection contains consumer evidence")
	}
	return nil
}

func (report RemovalInspection) ExecutionStopped() bool {
	return report.State == CommandNotCreated || report.State == CommandNotPermitted || report.State == CommandExited ||
		report.State == ConsumerInspected && report.Consumer != nil && report.Consumer.HelperStopped
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

// Checkpoint identifies the inspection that actually observed the external
// effect. It is distinct from the interrupted forward command's checkpoint.
func (report RemovalInspection) Checkpoint(input RecoveryInput) (workexecution.Checkpoint, error) {
	raw, evidence, err := EncodeRemovalInspection(input, report)
	clear(raw)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	if input.Attempt.StartedAt == nil {
		return workexecution.Checkpoint{}, invalid("an unstarted inspection cannot acknowledge observed runtime")
	}
	timestamp, err := ids.Timestamp(ids.KindWorkItem, input.Attempt.WorkItemID)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	return workexecution.Checkpoint{AttemptID: input.Attempt.ID, Sequence: 1,
		StageID:  ids.DeriveAt(ids.KindStep, timestamp, input.Attempt.WorkItemID, "attach-removal/inspection"),
		Evidence: evidence, RecordedAt: report.ObservedAt}, nil
}
