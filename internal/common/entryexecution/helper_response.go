package entryexecution

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func HelperObservation(input HelperInput, observation Observation) (*agentpb.EntryConfigurationWorkHelperResponse, error) {
	if input.Procedure == nil || input.Progress.Validate(input.Attempt, input.Procedure) != nil ||
		!input.Progress.InFlight || observation.EnvironmentID != input.Procedure.EnvironmentId || observation.Input != input.Attempt.Input ||
		observation.Stage != input.Progress.Stage || observation.At.IsZero() || observation.At.Location() != time.UTC || observation.At.Before(*input.Progress.BeganAt) {
		return nil, invalidLayout()
	}
	// An inspection may report that the expected effect is absent, but cannot
	// carry contradictory proofs or facts belonging to another stage.
	empty := Observation{EnvironmentID: observation.EnvironmentID, Input: observation.Input, Stage: observation.Stage, At: observation.At}
	if !observation.Satisfies(input.Progress.Stage) && (!input.InspectOnly || observation != empty) {
		return nil, invalidLayout()
	}
	raw, err := json.Marshal(observation)
	if err != nil {
		return nil, err
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil || len(raw) > MaximumProgressBytes {
		return nil, invalidLayout()
	}
	return &agentpb.EntryConfigurationWorkHelperResponse{Observation: raw}, nil
}

func ReadHelperObservation(input HelperInput, response *agentpb.EntryConfigurationWorkHelperResponse) (Observation, error) {
	if response == nil || executionplan.RejectUnknown(response) != nil ||
		len(response.Observation) == 0 || len(response.Observation) > MaximumProgressBytes {
		return Observation{}, invalidLayout()
	}
	observation, err := jcs.Decode[Observation](response.Observation)
	if err != nil {
		return Observation{}, err
	}
	expected, err := HelperObservation(input, observation)
	if err != nil || !bytes.Equal(expected.Observation, response.Observation) {
		return Observation{}, invalidLayout()
	}
	return observation, nil
}
