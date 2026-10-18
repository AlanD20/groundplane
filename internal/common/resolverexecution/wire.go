package resolverexecution

import (
	"bytes"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Assignment struct {
	Attempt  workexecution.Attempt
	Input    Input
	Recovery *Recovery
}

func DecodeAssignment(message *agentpb.ResolverWorkAssignment, identity workexecution.Identity) (Assignment, error) {
	if message == nil || executionplan.RejectUnknown(message) != nil {
		return Assignment{}, invalid("DNS resolver assignment is invalid")
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	if err != nil {
		return Assignment{}, err
	}
	input, reference, err := Decode(attempt.Input.ID, message.Input)
	if err != nil || reference != attempt.Input || attempt.FinishedAt != nil {
		return Assignment{}, invalid("DNS resolver assignment differs from its captured input")
	}
	layout, err := CaptureLayout(attempt, input)
	if err != nil {
		return Assignment{}, err
	}
	assignment := Assignment{Attempt: attempt, Input: input}
	if attempt.Direction == workexecution.Recovery {
		if len(message.Recovery) == 0 || len(message.Recovery) > MaximumReportBytes {
			return Assignment{}, invalid("DNS resolver cleanup assignment has no captured failure source")
		}
		source, err := jcs.Decode[Recovery](message.Recovery)
		if err != nil || source.ValidateFor(attempt, layout) != nil {
			return Assignment{}, invalid("DNS resolver cleanup assignment source is invalid")
		}
		raw, err := EncodeRecovery(source)
		if err != nil || !bytes.Equal(raw, message.Recovery) {
			return Assignment{}, invalid("DNS resolver cleanup source is not canonical")
		}
		assignment.Recovery = &source
	} else {
		if len(message.Recovery) != 0 {
			return Assignment{}, invalid("forward DNS resolver work cannot carry cleanup authority")
		}
	}
	return assignment, nil
}

func EncodeRecovery(source Recovery) ([]byte, error) {
	raw, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil || len(canonical) > MaximumReportBytes {
		return nil, invalid("DNS resolver cleanup source exceeds its bounded format")
	}
	return canonical, nil
}

func EncodeReference(reference workexecution.Input) (*agentpb.WorkInputReference, error) {
	if err := reference.Validate(); err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.WorkInputReference{InputId: reference.ID, Sha256: digest}, nil
}

func DecodeReference(reference *agentpb.WorkInputReference) (workexecution.Input, error) {
	if reference == nil || executionplan.RejectUnknown(reference) != nil {
		return workexecution.Input{}, invalid("DNS resolver evidence reference is invalid")
	}
	decoded := workexecution.Input{ID: reference.InputId, SHA256: hex.EncodeToString(reference.Sha256)}
	return decoded, decoded.Validate()
}

func CheckpointAck(attempt workexecution.Attempt, layout Layout, progress Progress, ack *agentpb.ResolverWorkCheckpointAck) error {
	if ack == nil || executionplan.RejectUnknown(ack) != nil {
		return invalid("DNS resolver checkpoint acknowledgement names a different assignment")
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, attempt) {
		return invalid("DNS resolver checkpoint acknowledgement names a different assignment")
	}
	raw, err := EncodeProgress(attempt, layout, progress)
	if err != nil || !bytes.Equal(raw, ack.Progress) {
		return invalid("DNS resolver checkpoint acknowledgement changed its saved progress")
	}
	cursor, err := progress.Checkpoint(attempt, layout)
	if err != nil {
		return err
	}
	evidence, err := DecodeReference(ack.Evidence)
	if err != nil || evidence != cursor.Evidence {
		return invalid("DNS resolver checkpoint acknowledgement changed its saved evidence")
	}
	return nil
}

func DecodeTerminal(attempt workexecution.Attempt, kind string, evidence workexecution.Input,
	ack *agentpb.ResolverWorkResultAck,
) (workexecution.Attempt, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.Kind != kind || ack.FinishedAt == nil ||
		ack.FinishedAt.CheckValid() != nil || ack.StartedAt != nil && ack.StartedAt.CheckValid() != nil {
		return workexecution.Attempt{}, invalid("DNS resolver terminal acknowledgement is invalid")
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, attempt) {
		return workexecution.Attempt{}, invalid("DNS resolver terminal acknowledgement names another assignment")
	}
	ref, err := DecodeReference(ack.Evidence)
	if err != nil || ref != evidence {
		return workexecution.Attempt{}, invalid("DNS resolver terminal acknowledgement changed its saved evidence")
	}
	terminal, finished := attempt, ack.FinishedAt.AsTime().UTC()
	terminal.Outcome, terminal.FinishedAt, terminal.Result = workexecution.Outcome(ack.Outcome), &finished, &ref
	if ack.StartedAt != nil {
		started := ack.StartedAt.AsTime().UTC()
		if attempt.StartedAt != nil && !attempt.StartedAt.Equal(started) {
			return workexecution.Attempt{}, invalid("DNS resolver terminal acknowledgement changed its actual start")
		}
		terminal.StartedAt = &started
	} else if attempt.StartedAt != nil {
		return workexecution.Attempt{}, invalid("DNS resolver terminal acknowledgement omitted its actual start")
	}
	if err := terminal.Validate(); err != nil {
		return workexecution.Attempt{}, err
	}
	if kind == "completion" && terminal.StartedAt == nil || kind == "failure" && terminal.Outcome == workexecution.Succeeded {
		return workexecution.Attempt{}, invalid("DNS resolver terminal outcome contradicts its report")
	}
	return terminal, nil
}
