package resolverexecution

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type HelperInput struct {
	Assignment  Assignment
	Layout      Layout
	Progress    *Progress
	InspectOnly bool
}

func OpenHelperRequest(request *agentpb.ResolverWorkHelperRequest) (HelperInput, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || request.Assignment == nil ||
		request.Assignment.Attempt == nil ||
		proto.Size(request) > MaximumInputBytes+4*MaximumReportBytes {
		return HelperInput{}, invalid("DNS resolver helper request is missing or oversized")
	}
	claimed := request.Assignment.Attempt
	identity := workexecution.Identity{
		Executor:   workexecution.Agent,
		HostID:     claimed.HostId,
		AgentID:    claimed.AgentId,
		Generation: claimed.ExecutorGeneration,
	}
	assignment, err := DecodeAssignment(request.Assignment, identity)
	if err != nil {
		return HelperInput{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, assignment.Attempt)
	if err != nil {
		return HelperInput{}, err
	}
	assignment.Attempt.StartedAt = &started
	layout, err := CaptureLayout(assignment.Attempt, assignment.Input)
	if err != nil {
		return HelperInput{}, err
	}
	input := HelperInput{Assignment: assignment, Layout: layout, InspectOnly: request.InspectOnly}
	if assignment.Recovery != nil {
		if request.Checkpoint != nil || request.InspectOnly {
			return HelperInput{}, invalid("DNS resolver cleanup cannot substitute a forward boundary")
		}
		return input, assignment.Recovery.ValidateFor(assignment.Attempt, layout)
	}
	if request.Checkpoint == nil {
		return HelperInput{}, invalid("DNS resolver helper has no acknowledged execution boundary")
	}
	progress, err := DecodeProgress(assignment.Attempt, layout, request.Checkpoint.Progress)
	if err != nil {
		return HelperInput{}, err
	}
	if !progress.InFlight {
		return HelperInput{}, invalid("DNS resolver helper stage is not active")
	}
	if err := CheckpointAck(assignment.Attempt, layout, progress, request.Checkpoint); err != nil {
		return HelperInput{}, err
	}
	input.Progress = &progress
	return input, nil
}

func HelperObservation(
	input HelperInput,
	observation Observation,
	proof *agentpb.DNSResolverObservationEvidence,
) (*agentpb.ResolverWorkHelperResponse, error) {
	if input.Assignment.Recovery != nil || input.Progress == nil ||
		input.Progress.Validate(input.Assignment.Attempt, input.Layout) != nil ||
		!input.Progress.InFlight ||
		observation.Input != input.Layout.Input ||
		observation.ComponentID != input.Layout.ComponentID ||
		observation.Stage != input.Progress.Stage ||
		!canonicalTime(observation.At) ||
		observation.At.Before(*input.Progress.BeganAt) {
		return nil, invalid("DNS resolver helper observation has no matching active boundary")
	}
	empty := Observation{
		Input:       input.Layout.Input,
		ComponentID: input.Layout.ComponentID,
		Stage:       input.Progress.Stage,
		At:          observation.At,
	}
	if !observation.Satisfies(input.Progress.Stage, input.Layout) && (!input.InspectOnly || observation != empty) {
		return nil, invalid("DNS resolver helper did not prove its selected stage")
	}
	if proof != nil {
		actual, err := ServingObservation(input.Assignment.Attempt, input.Assignment.Input, proof)
		if err != nil || observation != actual {
			return nil, invalid("DNS resolver helper proof differs from its serving observation")
		}
	} else if observation.Stage == VerifyDNS && observation.RuntimePresent {
		return nil, invalid("DNS resolver helper has no actual serving proof")
	}
	raw, err := json.Marshal(observation)
	if err != nil {
		return nil, err
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil || len(canonical) > MaximumReportBytes {
		return nil, invalid("DNS resolver observation exceeds its bound")
	}
	return &agentpb.ResolverWorkHelperResponse{Observation: canonical, ServingProof: proto.CloneOf(proof)}, nil
}

func ReadHelperObservation(
	input HelperInput,
	response *agentpb.ResolverWorkHelperResponse,
) (Observation, *agentpb.DNSResolverObservationEvidence, error) {
	if response == nil || executionplan.RejectUnknown(response) != nil || len(response.Observation) == 0 ||
		len(response.Observation) > MaximumReportBytes ||
		len(response.Restoration) != 0 {
		return Observation{}, nil, invalid("DNS resolver helper response is missing or invalid")
	}
	observed, err := jcs.Decode[Observation](response.Observation)
	if err != nil {
		return Observation{}, nil, err
	}
	expected, err := HelperObservation(input, observed, response.ServingProof)
	if err != nil || !proto.Equal(expected, response) {
		return Observation{}, nil, invalid("DNS resolver helper response differs from its canonical observation")
	}
	return observed, proto.CloneOf(response.ServingProof), nil
}

// RestorationObservation proves only the physical helper's scoped effects.
// HelpersStopped is deliberately absent: the parent must separately join it.
type RestorationObservation struct {
	Input                 workexecution.Input                     `json:"input"`
	ComponentID           string                                  `json:"component_id"`
	ForwardID             string                                  `json:"forward_id"`
	ConfigurationRestored bool                                    `json:"configuration_restored"`
	RuntimeRestored       bool                                    `json:"runtime_restored"`
	HostResolverRestored  bool                                    `json:"host_resolver_restored"`
	ServingProof          *agentpb.DNSResolverObservationEvidence `json:"serving_proof,omitempty"`
	ObservedAt            time.Time                               `json:"observed_at"`
}

func HelperRestoration(
	input HelperInput,
	observation RestorationObservation,
) (*agentpb.ResolverWorkHelperResponse, error) {
	assignment := input.Assignment
	if assignment.Recovery == nil || observation.Input != input.Layout.Input ||
		observation.ComponentID != input.Layout.ComponentID ||
		observation.ForwardID != assignment.Recovery.Forward.ID ||
		assignment.Attempt.StartedAt == nil ||
		!observation.ConfigurationRestored ||
		!observation.RuntimeRestored ||
		!observation.HostResolverRestored ||
		!canonicalTime(observation.ObservedAt) ||
		observation.ObservedAt.Before(*assignment.Attempt.StartedAt) {
		return nil, invalid("DNS resolver helper restoration has no exact captured authority")
	}
	previous := assignment.Input.Previous
	if !assignment.Input.PreviousEnabled && observation.ServingProof != nil || assignment.Input.PreviousEnabled &&
		(ValidateServingProof(*previous, observation.ServingProof) != nil || observation.ServingProof.ObservedAt.AsTime().Before(*assignment.Attempt.StartedAt) ||
			observation.ServingProof.ObservedAt.AsTime().After(observation.ObservedAt)) {
		return nil, invalid("DNS resolver helper did not prove its captured previous serving configuration")
	}
	raw, err := json.Marshal(observation)
	if err != nil {
		return nil, err
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil || len(canonical) > MaximumReportBytes {
		return nil, invalid("DNS resolver restoration observation exceeds its bound")
	}
	return &agentpb.ResolverWorkHelperResponse{Restoration: canonical}, nil
}

func ReadHelperRestoration(
	input HelperInput,
	response *agentpb.ResolverWorkHelperResponse,
) (RestorationObservation, error) {
	if response == nil || executionplan.RejectUnknown(response) != nil || len(response.Restoration) == 0 ||
		len(response.Restoration) > MaximumReportBytes ||
		len(response.Observation) != 0 ||
		response.ServingProof != nil {
		return RestorationObservation{}, invalid("DNS resolver helper restoration response is invalid")
	}
	observation, err := jcs.Decode[RestorationObservation](response.Restoration)
	if err != nil {
		return RestorationObservation{}, err
	}
	expected, err := HelperRestoration(input, observation)
	if err != nil || !bytes.Equal(expected.Restoration, response.Restoration) {
		return RestorationObservation{}, invalid("DNS resolver helper restoration response is not canonical")
	}
	return observation, nil
}
