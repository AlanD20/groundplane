package attachexecution

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func EncodeProvisionFacts(
	input ProvisionInput,
	output backinghook.Output,
) (*agentpb.AttachProvisionFactsRequest, error) {
	if input.Validate() != nil || input.Provision.GetCustomAttach() == nil {
		return nil, invalid("Attach facts require a captured Custom hook")
	}
	_, _, schema, err := executionplan.DecodeBackingHookProcedure(input.Provision.GetCustomAttach())
	if err != nil {
		return nil, err
	}
	if err := backinghook.ValidateOutput(schema, output); err != nil {
		return nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	request := &agentpb.AttachProvisionFactsRequest{Attempt: identity}
	// Schema order makes replay independent of map or caller ordering.
	for _, definition := range schema {
		for _, fact := range output.Facts {
			if fact.Key == definition.Key {
				request.Facts = append(
					request.Facts,
					&agentpb.BackingHookValue{Key: fact.Key, Value: append([]byte(nil), fact.Value...)},
				)
				break
			}
		}
	}
	return request, nil
}

func DecodeProvisionFacts(
	input ProvisionInput,
	request *agentpb.AttachProvisionFactsRequest,
) (backinghook.Output, []byte, error) {
	if input.Validate() != nil || input.Provision.GetCustomAttach() == nil || request == nil ||
		executionplan.RejectUnknown(request) != nil || proto.Size(request) > executionplan.MaximumPlanBytes {
		return backinghook.Output{}, nil, invalid("Attach facts request is malformed or oversized")
	}
	claimed, err := workexecution.DecodeAgentAttempt(request.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, input.Attempt) {
		return backinghook.Output{}, nil, invalid("Attach facts belong to another execution")
	}
	_, _, schema, err := executionplan.DecodeBackingHookProcedure(input.Provision.GetCustomAttach())
	if err != nil {
		return backinghook.Output{}, nil, err
	}
	if len(request.Facts) != len(schema) {
		return backinghook.Output{}, nil, invalid("Attach facts do not match the captured schema")
	}
	output := backinghook.Output{}
	for index, definition := range schema {
		fact := request.Facts[index]
		if fact == nil || fact.Key != definition.Key {
			output.Clear()
			return backinghook.Output{}, nil, invalid("Attach facts are not in captured schema order")
		}
		output.Facts = append(
			output.Facts,
			backinghook.Fact{Key: fact.Key, Secret: definition.Secret, Value: append([]byte(nil), fact.Value...)},
		)
	}
	if err := backinghook.ValidateOutput(schema, output); err != nil {
		output.Clear()
		return backinghook.Output{}, nil, err
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		output.Clear()
		return backinghook.Output{}, nil, invalid("Attach facts cannot be encoded")
	}
	digest := sha256.Sum256(raw)
	clear(raw)
	return output, append([]byte(nil), digest[:]...), nil
}

func ValidateProvisionFactsAck(
	input ProvisionInput,
	request *agentpb.AttachProvisionFactsRequest,
	ack *agentpb.AttachProvisionFactsAck,
) error {
	if ack == nil || executionplan.RejectUnknown(ack) != nil {
		return invalid("Attach facts acknowledgement is malformed")
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, input.Attempt) {
		return invalid("Attach facts acknowledgement belongs to another execution")
	}
	output, digest, err := DecodeProvisionFacts(input, request)
	output.Clear()
	if err != nil {
		return err
	}
	if !bytes.Equal(digest, ack.ResultSha256) {
		return invalid("Attach facts acknowledgement differs from the hook output")
	}
	return nil
}

func ClearProvisionFacts(request *agentpb.AttachProvisionFactsRequest) {
	if request != nil {
		for _, fact := range request.Facts {
			if fact != nil {
				clear(fact.Value)
				fact.Value = nil
			}
		}
	}
}
