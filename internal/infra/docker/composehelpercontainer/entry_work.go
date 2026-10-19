package composehelpercontainer

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The Agent holds exclusive attempt ownership across this call. Inspection
// stops the exact previous helper first; neither timeout nor lost transport
// alone is evidence that a Docker mutation has stopped.
func (executor *Executor) ExecuteEntryConfigurationWork(
	ctx context.Context,
	request *agentpb.EntryConfigurationWorkHelperRequest,
) (*agentpb.EntryConfigurationWorkHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Entry helper container is not configured")
	}
	input, err := entryexecution.OpenHelperRequest(request)
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
	name := "gp-entry-executor-" + input.Attempt.ID
	labels := releaseHelperLabels(input.Attempt)
	labels["io.groundplane.role"] = "entry-executor"
	if input.Recovery != nil {
		if err := executor.StopEntryConfigurationWork(bounded, request); err != nil {
			return nil, err
		}
	}
	if input.InspectOnly {
		if err := executor.stopNamedHelper(bounded, name, labels); err != nil {
			return nil, err
		}
	}
	framed, err := composehelper.MarshalEntryConfigurationWorkRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	options := createOptions(executor.image, "entry-work-helper", nil)
	options.Name, options.Config.Labels = name, labels
	raw, err := executor.executeFrame(bounded, options, framed)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	return composehelper.ReadEntryConfigurationWorkResponse(bounded, bytes.NewReader(raw), input)
}
