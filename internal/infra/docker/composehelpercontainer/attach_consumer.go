package composehelpercontainer

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) ExecuteAttachConsumer(ctx context.Context, request *agentpb.AttachConsumerHelperRequest) (*agentpb.AttachConsumerHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Detach consumer helper container is not configured")
	}
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
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	name := "gp-attach-consumer-" + input.Attempt.ID
	labels := releaseHelperLabels(input.Attempt)
	labels["io.groundplane.role"] = "attach-consumer"
	if input.Forward != nil {
		forward := *input.Forward
		priorLabels := releaseHelperLabels(forward)
		priorLabels["io.groundplane.role"] = "attach-consumer"
		if err := executor.stopNamedHelper(bounded, "gp-attach-consumer-"+forward.ID, priorLabels); err != nil {
			return nil, err
		}
	}
	if input.InspectOnly {
		if err := executor.stopNamedHelper(bounded, name, labels); err != nil {
			return nil, err
		}
	}
	framed, err := composehelper.MarshalAttachConsumerRequest(bounded, request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	options := createOptions(executor.image, nil)
	options.Name, options.Config.Labels = name, labels
	options.Config.Cmd = []string{"attach-consumer-helper"}
	raw, err := executor.executeFrame(bounded, options, framed)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	return composehelper.ReadAttachConsumerResponse(bounded, bytes.NewReader(raw), input)
}
