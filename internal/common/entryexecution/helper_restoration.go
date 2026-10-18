package entryexecution

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ConsumerRestoration proves only workload restoration. File verification and
// helper termination are separately owned by the Agent recovery coordinator.
type ConsumerRestoration struct {
	AttemptID string              `json:"attempt_id"`
	Input     workexecution.Input `json:"input"`
	Restored  bool                `json:"restored"`
	At        time.Time           `json:"at"`
}

func HelperRestoration(input HelperInput, observed ConsumerRestoration) (*agentpb.EntryConfigurationWorkHelperResponse, error) {
	if input.Recovery == nil || input.Recovery.ValidateFor(input.Attempt, input.Procedure) != nil || input.Attempt.StartedAt == nil ||
		observed.AttemptID != input.Attempt.ID || observed.Input != input.Attempt.Input || observed.At.IsZero() ||
		observed.At.Location() != time.UTC || observed.At.Before(*input.Attempt.StartedAt) || !observed.Restored && !input.InspectOnly {
		return nil, invalidLayout()
	}
	raw, err := json.Marshal(observed)
	if err != nil {
		return nil, err
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	return &agentpb.EntryConfigurationWorkHelperResponse{Observation: raw}, nil
}

func ReadHelperRestoration(input HelperInput, response *agentpb.EntryConfigurationWorkHelperResponse) (ConsumerRestoration, error) {
	if response == nil || executionplan.RejectUnknown(response) != nil || len(response.Observation) == 0 || len(response.Observation) > MaximumProgressBytes {
		return ConsumerRestoration{}, invalidLayout()
	}
	observed, err := jcs.Decode[ConsumerRestoration](response.Observation)
	if err != nil {
		return ConsumerRestoration{}, err
	}
	expected, err := HelperRestoration(input, observed)
	if err != nil || !bytes.Equal(expected.Observation, response.Observation) {
		return ConsumerRestoration{}, invalidLayout()
	}
	return observed, nil
}
