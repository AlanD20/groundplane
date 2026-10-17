package composehelper

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ExecuteAttachConsumer(ctx context.Context, taskRunner runner.Runner, request *agentpb.AttachConsumerHelperRequest) (*agentpb.AttachConsumerHelperResponse, error) {
	input, err := attachexecution.OpenConsumerHelperRequest(ctx, request)
	if err != nil {
		return nil, err
	}
	bounded, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	if input.InspectOnly {
		cancel()
		bounded, cancel = context.WithTimeout(ctx, 30*time.Second)
	}
	defer cancel()
	var observation AttachConsumerObservation
	if input.InspectOnly {
		observation, err = ObserveAttachConsumerDocker(bounded, taskRunner, input.Consumer)
	} else {
		observation, err = ApplyAttachConsumerDocker(bounded, taskRunner, input.Consumer)
	}
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(input.Attempt.Input.SHA256)
	if err != nil {
		return nil, err
	}
	response := &agentpb.AttachConsumerHelperResponse{ServiceId: observation.ServiceID, NetworkId: observation.NetworkID,
		Input:    &agentpb.WorkInputReference{InputId: input.Attempt.Input.ID, Sha256: digest},
		Detached: observation.Detached, ObservedAt: timestamppb.New(observation.At), Inspection: input.InspectOnly}
	if err := attachexecution.ValidateConsumerHelperResponse(input, response); err != nil {
		return nil, err
	}
	return response, nil
}
