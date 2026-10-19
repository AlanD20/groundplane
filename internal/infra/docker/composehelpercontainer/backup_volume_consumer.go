package composehelpercontainer

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) ExecuteBackupConsumer(
	ctx context.Context,
	request *agentpb.BackupConsumerHelperRequest,
) (*agentpb.BackupConsumerHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Backup Volume helper container is not configured")
	}
	input, err := executionplan.OpenBackupConsumerHelper(ctx, request)
	if err != nil {
		return nil, err
	}
	deadline := input.Consumer.Attempt.Deadline
	if sourceDeadline := time.Unix(0, int64(input.Consumer.Source.StepDeadlineUnixNano)); sourceDeadline.Before(
		deadline,
	) {
		deadline = sourceDeadline
	}
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if input.Inspection {
		var stop context.CancelFunc
		bounded, stop = context.WithTimeout(bounded, 30*time.Second)
		defer stop()
	}
	name := "gp-backup-consumer-" + input.Consumer.Attempt.ID
	labels := releaseHelperLabels(input.Consumer.Attempt)
	labels["io.groundplane.role"] = "backup-consumer"
	if input.Inspection {
		if err := executor.stopNamedHelper(bounded, name, labels); err != nil {
			return nil, err
		}
	}
	framed, err := composehelper.MarshalBackupConsumerRequest(bounded, request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	options := createOptions(executor.image, "backup-consumer-helper", nil)
	options.Name, options.Config.Labels = name, labels
	raw, err := executor.executeFrame(bounded, options, framed)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	return composehelper.ReadBackupConsumerResponse(bounded, bytes.NewReader(raw), input)
}
