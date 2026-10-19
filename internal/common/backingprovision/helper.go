package backingprovision

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// HelperInput validates the captured boundary, not the transport's authenticity.
// The launcher must additionally hold the current host process/attempt lease.
type HelperInput struct {
	Assignment  Assignment
	Layout      Layout
	Progress    *Progress
	InspectOnly bool
}

func OpenHelperRequest(request *agentpb.BackingProvisionHelperRequest) (HelperInput, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || request.Assignment == nil ||
		request.Assignment.Attempt == nil ||
		proto.Size(request) > MaximumInputBytes+6*MaximumProgressBytes+4096 {
		return HelperInput{}, invalid("backing creation helper request is missing or oversized")
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
	layout, err := CaptureLayout(assignment.Attempt, assignment.Procedure)
	if err != nil {
		return HelperInput{}, err
	}
	input := HelperInput{Assignment: assignment, Layout: layout, InspectOnly: request.InspectOnly}
	if assignment.Recovery != nil {
		if request.Report != nil || request.Acknowledgement != nil || request.InspectOnly {
			return HelperInput{}, invalid("backing creation cleanup cannot substitute a forward checkpoint")
		}
		return input, assignment.Recovery.ValidateFor(assignment.Attempt, layout)
	}
	if request.Report == nil || request.Acknowledgement == nil || request.Acknowledgement.Evidence == nil ||
		!bytes.Equal(request.Report.Progress, request.Acknowledgement.Progress) {
		return HelperInput{}, invalid("backing creation helper requires its acknowledged progress boundary")
	}
	for _, attempt := range []*agentpb.WorkAttemptIdentity{request.Report.Attempt, request.Acknowledgement.Attempt} {
		decoded, err := workexecution.DecodeAgentAttempt(attempt, identity)
		if err != nil || !workexecution.SameAssignment(decoded, assignment.Attempt) {
			return HelperInput{}, invalid("backing creation helper checkpoint belongs to another assignment")
		}
	}
	progress, err := DecodeProgress(assignment.Attempt, layout, request.Report.Progress)
	if err != nil {
		return HelperInput{}, err
	}
	if !progress.InFlight {
		return HelperInput{}, invalid("backing creation helper has no active effect boundary")
	}
	checkpoint, err := progress.Checkpoint(assignment.Attempt, layout)
	if err != nil || checkpoint.Evidence.ID != request.Acknowledgement.Evidence.InputId ||
		checkpoint.Evidence.SHA256 != hex.EncodeToString(request.Acknowledgement.Evidence.Sha256) {
		return HelperInput{}, invalid("backing creation helper checkpoint evidence changed")
	}
	input.Progress = &progress
	return input, nil
}

func HelperObservation(
	input HelperInput,
	observation Observation,
	runtime *backingruntimefact.Observation,
) (*agentpb.BackingProvisionHelperResponse, error) {
	if input.Assignment.Recovery != nil || input.Progress == nil ||
		input.Progress.Validate(input.Assignment.Attempt, input.Layout) != nil ||
		!input.Progress.InFlight ||
		observation.ServiceID != input.Layout.ServiceID ||
		observation.Input != input.Layout.Input ||
		observation.Stage != input.Progress.Stage ||
		observation.At.IsZero() ||
		observation.At.Location() != time.UTC ||
		observation.At.Before(*input.Progress.BeganAt) {
		return nil, invalid("backing creation helper observation has no matching active boundary")
	}
	empty := Observation{
		ServiceID: observation.ServiceID,
		Input:     observation.Input,
		Stage:     observation.Stage,
		At:        observation.At,
	}
	if !observation.Satisfies(input.Progress.Stage, input.Layout) && (!input.InspectOnly || observation != empty) {
		return nil, invalid("backing creation helper did not prove its selected stage")
	}
	raw, err := json.Marshal(observation)
	if err != nil {
		return nil, err
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil || len(canonical) > MaximumProgressBytes {
		return nil, invalid("backing creation helper observation exceeds its bounded format")
	}
	response := &agentpb.BackingProvisionHelperResponse{Observation: canonical}
	if runtime != nil {
		if input.Progress.Stage != VerifyRuntime || !observation.RuntimeVerified ||
			runtime.ContainerID != observation.ContainerID ||
			!runtime.ObservedAt.Equal(observation.At) {
			return nil, invalid("backing creation runtime proof belongs to another stage or container")
		}
		workload, err := backingruntimefact.Workload(input.Assignment.Procedure.Runtime, input.Layout.ServiceID,
			input.Assignment.Attempt.Input.ID, input.Assignment.Procedure.RenderGeneration)
		if err != nil ||
			!backingruntimefact.MatchesObservation(*runtime, input.Assignment.Procedure.Runtime, workload) {
			return nil, invalid("backing creation runtime proof differs from its captured artifact")
		}
		response.RuntimeObservation, err = EncodeRuntimeObservation(*runtime)
		if err != nil {
			return nil, err
		}
	} else if input.Progress.Stage == VerifyRuntime && observation.RuntimeVerified {
		return nil, invalid("backing creation final verification is missing its actual runtime observation")
	}
	return response, nil
}

func ReadHelperObservation(
	input HelperInput,
	response *agentpb.BackingProvisionHelperResponse,
) (Observation, *backingruntimefact.Observation, error) {
	if response == nil || executionplan.RejectUnknown(response) != nil || len(response.Observation) == 0 ||
		len(response.Observation) > MaximumProgressBytes || len(response.RecoveryReport) != 0 {
		return Observation{}, nil, invalid("backing creation helper response is missing or invalid")
	}
	observation, err := jcs.Decode[Observation](response.Observation)
	if err != nil {
		return Observation{}, nil, err
	}
	var runtime *backingruntimefact.Observation
	if len(response.RuntimeObservation) != 0 {
		observed, err := DecodeRuntimeObservation(response.RuntimeObservation)
		if err != nil {
			return Observation{}, nil, err
		}
		runtime = &observed
	}
	expected, err := HelperObservation(input, observation, runtime)
	if err != nil || !proto.Equal(expected, response) {
		return Observation{}, nil, invalid("backing creation helper response differs from its canonical proof")
	}
	return observation, runtime, nil
}
