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

const ProvisionCommandCompleted InspectionState = "provision_command_completed"
const ProvisionConsumerInspected InspectionState = "provision_consumer_inspected"

// ProvisionInspection proves execution state, not reversal of credentials,
// databases or grants. The failed binding retains those effects for the operator.
type ProvisionInspection struct {
	AttemptID        string                   `json:"attempt_id"`
	ForwardID        string                   `json:"forward_id"`
	Failure          workexecution.Input      `json:"failure"`
	CommandOrdinal   uint32                   `json:"command_ordinal"`
	PreparedCommands uint32                   `json:"prepared_commands"`
	State            InspectionState          `json:"state"`
	ContainerID      string                   `json:"container_id,omitempty"`
	ExecID           string                   `json:"exec_id,omitempty"`
	ExitCode         *int                     `json:"exit_code,omitempty"`
	ObservedAt       time.Time                `json:"observed_at"`
	Consumer         *ProvisionConsumerResult `json:"consumer,omitempty"`
	HelperStopped    bool                     `json:"helper_stopped,omitempty"`
}

func (report ProvisionInspection) Validate(input ProvisionRecoveryInput) error {
	if input.Validate() != nil || input.Attempt.StartedAt == nil && report.State != InspectionInterrupted ||
		report.AttemptID != input.Attempt.ID || report.ForwardID != input.Source.Forward.ID || report.Failure != input.Source.Evidence ||
		report.CommandOrdinal != uint32(
			(input.Source.Failure.Checkpoint.Sequence-1)/2,
		) || report.PreparedCommands > 16 ||
		report.ObservedAt.IsZero() || report.ObservedAt.Location() != time.UTC || report.ObservedAt.Before(input.Attempt.AssignedAt) ||
		input.Attempt.StartedAt != nil && report.ObservedAt.Before(*input.Attempt.StartedAt) {
		return invalid("Attach provisioning inspection does not match its captured command")
	}
	completed := input.Source.Failure.Checkpoint.Sequence%2 == 0
	if input.RequiresConsumer() && report.State != ProvisionConsumerInspected && report.State != CommandJournalLost &&
		report.State != InspectionInterrupted {
		return invalid(
			"Attach connection inspection requires network-helper evidence, not backing-container exec state",
		)
	}
	if report.State != ProvisionConsumerInspected && (report.Consumer != nil || report.HelperStopped) {
		return invalid("Attach inspection contains unrelated network evidence")
	}
	switch report.State {
	case ProvisionConsumerInspected:
		consumer, authority := report.Consumer, input.Source.Failure.Authority
		if !input.RequiresConsumer() || consumer == nil || !report.HelperStopped || report.PreparedCommands != 0 ||
			report.ContainerID != "" ||
			report.ExecID != "" ||
			report.ExitCode != nil ||
			consumer.NetworkID != authority.ConsumerNetworkID ||
			consumer.ServiceID != authority.ConsumerServiceID ||
			consumer.CandidateSHA256 != authority.ConsumerCandidateSHA256 ||
			!consumer.ObservedAt.Equal(report.ObservedAt) {
			return invalid("Attach connection inspection has incomplete helper or membership evidence")
		}
	case ProvisionCommandCompleted:
		if !completed || report.PreparedCommands != 0 || report.ContainerID != "" || report.ExecID != "" ||
			report.ExitCode != nil {
			return invalid("Attach completion inspection requires the acknowledged command result")
		}
	case CommandNotCreated, CommandJournalLost, InspectionInterrupted:
		if report.State == CommandNotCreated && completed || report.PreparedCommands != 0 || report.ContainerID != "" ||
			report.ExecID != "" ||
			report.ExitCode != nil {
			return invalid("Attach inspection claims command state without its journal")
		}
	case CommandNotPermitted, CommandExited, CommandRunning, CommandUnobservable:
		if completed || report.PreparedCommands == 0 || !ids.ValidContainerID(report.ContainerID) ||
			!ids.ValidContainerID(report.ExecID) ||
			(report.State == CommandExited) != (report.ExitCode != nil) {
			return invalid("Attach provisioning inspection process evidence is incomplete")
		}
	default:
		return invalid("Attach provisioning inspection state is unsupported")
	}
	return nil
}

func (report ProvisionInspection) ExecutionStopped() bool {
	return report.State == ProvisionCommandCompleted ||
		report.State == ProvisionConsumerInspected && report.HelperStopped ||
		report.State == CommandNotCreated ||
		report.State == CommandNotPermitted ||
		report.State == CommandExited
}

func EncodeProvisionInspection(
	input ProvisionRecoveryInput,
	report ProvisionInspection,
) ([]byte, workexecution.Input, error) {
	if err := report.Validate(input); err != nil {
		return nil, workexecution.Input{}, err
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return nil, workexecution.Input{}, invalid("Attach provisioning inspection cannot be encoded")
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
	return raw, workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, at, input.Attempt.ID, "attach-provision/inspection"),
		SHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func DecodeProvisionInspection(
	input ProvisionRecoveryInput,
	raw []byte,
	evidence workexecution.Input,
) (ProvisionInspection, error) {
	if len(raw) == 0 || len(raw) > MaximumProvisionRecoveryBytes {
		return ProvisionInspection{}, invalid("Attach provisioning inspection is missing or oversized")
	}
	report, err := jcs.Decode[ProvisionInspection](raw)
	if err != nil {
		return ProvisionInspection{}, invalid("Attach provisioning inspection is malformed")
	}
	expected, reference, err := EncodeProvisionInspection(input, report)
	if err != nil || reference != evidence || !bytes.Equal(raw, expected) {
		return ProvisionInspection{}, invalid("Attach provisioning inspection differs from its retained evidence")
	}
	return report, nil
}
