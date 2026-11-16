package releaseruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
)

// Flush the saved terminal observation before generic failure settlement. A lost
// upload leaves the original attempt/journal available for reconnect delivery.
func (executor *Executor) deliverReadinessFailure(
	ctx context.Context,
	input releaseexecution.Input,
	exchange Exchange,
) error {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	captured, record, err := executor.journal.ReadinessFailure(bounded, input)
	if err != nil || record == nil {
		return err
	}
	delivery, err := checkpointdelivery.NewEffect(captured, executor.journal, exchange.EffectEvidence)
	if err != nil {
		return err
	}
	// Resume returns the known operation error after successful proof delivery;
	// only transport/journal errors block settlement. Send is exposed separately
	// so a definite failed action can never be confirmed as successful.
	return delivery.DeliverFailure(bounded, record.Boundary)
}
