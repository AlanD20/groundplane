package attachruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The helper owns endpoint mutation and inspection; it never restarts workloads.
// On reconnect it stops the prior helper before observing all captured endpoints.
func (host *BackingHost) connectConsumer(
	ctx context.Context,
	input attachexecution.ProvisionInput,
	progress attachexecution.ProvisionProgress,
	inspectOnly bool,
) (attachexecution.ProvisionCommandResult, error) {
	if err := host.ownership.Check(); err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	request, err := attachexecution.EncodeProvisionConsumerHelper(input, progress, inspectOnly)
	if err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	captured, err := attachexecution.OpenConsumerHelperRequest(ctx, request)
	if err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	response, err := host.consumer.ExecuteAttachConsumer(ctx, request)
	if err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	if err := attachexecution.ValidateConsumerHelperResponse(captured, response); err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	if !response.Connected {
		return attachexecution.ProvisionCommandResult{}, errs.New(
			errs.KindStateConflict,
			"Attach connection was interrupted before all captured workloads connected; explicit recovery is required",
		)
	}
	authority, err := input.Authority()
	if err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	result := attachexecution.ProvisionCommandResult{
		Ordinal:    progress.Completed,
		StartedAt:  *progress.BeganAt,
		FinishedAt: time.Now().UTC(),
		Consumer: &attachexecution.ProvisionConsumerResult{NetworkID: response.NetworkId, ServiceID: response.ServiceId,
			CandidateSHA256: authority.ConsumerCandidateSHA256, ObservedAt: response.ObservedAt.AsTime(), Connected: true},
	}
	if _, err := progress.Confirm(input.Attempt, authority, result, result.FinishedAt); err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	storage, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := host.journal.SaveAttachProvisionCommandResult(storage, input, result); err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	return result, nil
}

func (host *BackingHost) inspectConsumerRecovery(
	ctx context.Context,
	input attachexecution.ProvisionRecoveryInput,
) (attachexecution.ProvisionInspection, error) {
	request, err := attachexecution.EncodeProvisionConsumerRecoveryHelper(input)
	if err != nil {
		return attachexecution.ProvisionInspection{}, err
	}
	captured, err := attachexecution.OpenConsumerHelperRequest(ctx, request)
	if err != nil {
		return attachexecution.ProvisionInspection{}, err
	}
	response, err := host.consumer.ExecuteAttachConsumer(ctx, request)
	if err != nil {
		return attachexecution.ProvisionInspection{}, err
	}
	if err := attachexecution.ValidateConsumerHelperResponse(captured, response); err != nil {
		return attachexecution.ProvisionInspection{}, err
	}
	if err := host.ownership.Check(); err != nil {
		return attachexecution.ProvisionInspection{}, err
	}
	authority := input.Source.Failure.Authority
	report := attachexecution.ProvisionInspection{AttemptID: input.Attempt.ID, ForwardID: input.Source.Forward.ID,
		Failure: input.Source.Evidence, CommandOrdinal: authority.Commands - 1, State: attachexecution.ProvisionConsumerInspected,
		ObservedAt: response.ObservedAt.AsTime(), HelperStopped: true,
		Consumer: &attachexecution.ProvisionConsumerResult{NetworkID: response.NetworkId, ServiceID: response.ServiceId,
			CandidateSHA256: authority.ConsumerCandidateSHA256, ObservedAt: response.ObservedAt.AsTime(), Connected: response.Connected}}
	return report, report.Validate(input)
}
