package serviceobservation

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	wire "github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// A replica's bounded container metadata fits within 2 KiB, including protobuf
// framing and its Service row. Bound expected replies as well as request bytes;
// a page of 200 sixteen-replica Services cannot fit into one 256 KiB exchange.
const maximumObservedReplicasPerExchange = wire.MaximumEnvelopeBytes / 2048

func (observer *Observer) observeTargets(
	ctx context.Context,
	agentID string,
	targets []*agentpb.ServiceObservationTarget,
	replicas []uint32,
) ([]*agentpb.ServiceObservationRow, error) {
	rows := make([]*agentpb.ServiceObservationRow, 0, len(targets))
	for start := 0; start < len(targets); {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		request := &agentpb.ObserveServices{RequestId: ids.NewULID()}
		envelope := &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_ObserveServices{ObserveServices: request},
		}
		end, expected := start, uint64(0)
		for end < len(targets) {
			if end > start && expected+uint64(replicas[end]) > maximumObservedReplicasPerExchange {
				break
			}
			request.Targets = targets[start : end+1]
			if proto.Size(envelope) > wire.MaximumEnvelopeBytes {
				request.Targets = targets[start:end]
				break
			}
			expected += uint64(replicas[end])
			end++
		}
		if start == end {
			return nil, errs.New(errs.KindValidationFailed, "Service observation target exceeds the channel limit")
		}
		// The channel owns the real correlation id and connection fence. Every
		// batch shares the caller's deadline; none is retried after failure.
		result, err := observer.channel.ObserveServices(ctx, agentID, request.Targets)
		if err != nil {
			return nil, err
		}
		request.RequestId = result.GetRequestId()
		if err := wire.ValidateResult(request, result); err != nil {
			return nil, err
		}
		rows = append(rows, result.Observations...)
		start = end
	}
	return rows, nil
}
