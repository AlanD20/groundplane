package composehelpercontainer

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The Agent holds exclusive attempt ownership across this call. Inspection
// stops the exact previous helper first; neither timeout nor lost transport
// alone is evidence that a Docker mutation has stopped.
func (executor *Executor) ExecuteComponentWork(
	ctx context.Context,
	request *agentpb.ComponentWorkHelperRequest,
) (*agentpb.ComponentWorkHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Component helper container is not configured")
	}
	input, err := componentexecution.OpenHelperRequest(request)
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
	name := "gp-component-executor-" + input.Attempt.ID
	labels := releaseHelperLabels(input.Attempt)
	labels["io.groundplane.role"] = "component-executor"
	if input.Recovery != nil {
		forward := input.Recovery.Forward
		priorLabels := releaseHelperLabels(forward)
		priorLabels["io.groundplane.role"] = "component-executor"
		if err := executor.stopNamedHelper(bounded, "gp-component-executor-"+forward.ID, priorLabels); err != nil {
			return nil, err
		}
	}
	if input.InspectOnly || input.Recovery != nil {
		if err := executor.stopNamedHelper(bounded, name, labels); err != nil {
			return nil, err
		}
	}
	framed, err := composehelper.MarshalComponentWorkRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	artifact := input.Procedure.Candidate
	if artifact == nil || input.Recovery != nil {
		artifact = input.Procedure.Predecessor.Runtime
	}
	options := createOptions(executor.image, "component-work-helper", artifact)
	options.Name, options.Config.Labels = name, labels
	raw, err := executor.executeFrame(bounded, options, framed)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	return composehelper.ReadComponentWorkResponse(bounded, bytes.NewReader(raw), input)
}
