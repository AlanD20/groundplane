package hierarchyruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ConsumerHelper interface {
	ExecuteAttachConsumer(context.Context, *agentpb.AttachConsumerHelperRequest) (*agentpb.AttachConsumerHelperResponse, error)
}

// AttachRuntimeHost uses one journal and process lease for credential commands
// and consumer network changes. The command's captured type selects its backend.
type AttachRuntimeHost struct {
	backing   *BackingHost
	journal   *agentworkjournal.Journal
	helper    ConsumerHelper
	ownership *agentownership.Lease
}

func NewAttachRuntimeHost(backing *BackingHost, journal *agentworkjournal.Journal, helper ConsumerHelper, ownership *agentownership.Lease) (*AttachRuntimeHost, error) {
	if backing == nil || journal == nil || helper == nil || backing.journal != journal || backing.ownership != ownership {
		return nil, errs.New(errs.KindInternal, "Attach runtime requires one journal, process lease and its typed executors")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &AttachRuntimeHost{backing: backing, journal: journal, helper: helper, ownership: ownership}, nil
}

func (host *AttachRuntimeHost) ExecuteRemovalCommand(ctx context.Context, input attachexecution.RemovalInput,
	progress attachexecution.RemovalProgress, inspectOnly bool,
) (attachexecution.RemovalCommandResult, error) {
	ordinal, consumer := input.ConsumerCommandOrdinal()
	if !consumer || ordinal != progress.Completed {
		return host.backing.ExecuteRemovalCommand(ctx, input, progress, inspectOnly)
	}
	if ctx == nil || progress.Validate(input) != nil || !progress.InFlight {
		return attachexecution.RemovalCommandResult{}, errs.New(errs.KindStateConflict, "Detach consumer requires its acknowledged command boundary")
	}
	if err := host.ownership.Check(); err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	saved, err := host.journal.AttachRemoval(ctx, input)
	if err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	if saved == nil || !saved.Acknowledged || len(saved.Executions) != 0 {
		return attachexecution.RemovalCommandResult{}, errs.New(errs.KindStateConflict, "Detach consumer has no matching saved authorization")
	}
	want, err := (attachexecution.RemovalReport{Progress: progress}).Checkpoint(input)
	if err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	got, err := saved.Report.Checkpoint(input)
	if err != nil || got.Sequence != want.Sequence || got.StageID != want.StageID || got.Evidence != want.Evidence ||
		!got.RecordedAt.Equal(want.RecordedAt) {
		return attachexecution.RemovalCommandResult{}, errs.New(errs.KindStateConflict, "Detach consumer differs from its acknowledged checkpoint")
	}
	if saved.CommandResult != nil {
		return *saved.CommandResult, nil
	}
	request, err := attachexecution.EncodeConsumerHelperRequest(input, progress, inspectOnly)
	if err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	captured, err := attachexecution.OpenConsumerHelperRequest(ctx, request)
	if err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	response, err := host.helper.ExecuteAttachConsumer(ctx, request)
	if err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	if err := attachexecution.ValidateConsumerHelperResponse(captured, response); err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	if !response.Detached {
		return attachexecution.RemovalCommandResult{}, errs.New(errs.KindStateConflict, "Detach consumer transition was interrupted before endpoint removal; explicit recovery is required")
	}
	if err := host.ownership.Check(); err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	result := attachexecution.RemovalCommandResult{Ordinal: progress.Completed, StartedAt: *progress.BeganAt,
		FinishedAt: time.Now().UTC(), Consumer: &attachexecution.ConsumerResult{
			NetworkID: response.NetworkId, ServiceID: response.ServiceId, Detached: true,
			CandidateSHA256: saved.Authority.ConsumerCandidateSHA256, ObservedAt: response.ObservedAt.AsTime(),
		}}
	storage, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := host.journal.SaveAttachCommandResult(storage, input, result); err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	return result, nil
}

func (host *AttachRuntimeHost) InspectRemoval(ctx context.Context, input attachexecution.RecoveryInput) (attachexecution.RemovalInspection, error) {
	if input.RequiresConsumer() {
		return host.inspectConsumerRecovery(ctx, input)
	}
	return host.backing.InspectRemoval(ctx, input)
}

func (host *AttachRuntimeHost) inspectConsumerRecovery(ctx context.Context, input attachexecution.RecoveryInput) (attachexecution.RemovalInspection, error) {
	if err := input.Validate(); err != nil {
		return attachexecution.RemovalInspection{}, err
	}
	if err := host.ownership.Check(); err != nil {
		return attachexecution.RemovalInspection{}, err
	}
	request, err := attachexecution.EncodeConsumerRecoveryHelperRequest(input)
	if err != nil {
		return attachexecution.RemovalInspection{}, err
	}
	captured, err := attachexecution.OpenConsumerHelperRequest(ctx, request)
	if err != nil {
		return attachexecution.RemovalInspection{}, err
	}
	response, err := host.helper.ExecuteAttachConsumer(ctx, request)
	if err != nil {
		return attachexecution.RemovalInspection{}, err
	}
	if err := attachexecution.ValidateConsumerHelperResponse(captured, response); err != nil {
		return attachexecution.RemovalInspection{}, err
	}
	if err := host.ownership.Check(); err != nil {
		return attachexecution.RemovalInspection{}, err
	}
	report := attachexecution.RemovalInspection{AttemptID: input.Attempt.ID, ForwardID: input.Source.Forward.ID,
		Failure: input.Source.Evidence, CommandOrdinal: input.Source.Failure.Authority.ConsumerOrdinal,
		State: attachexecution.ConsumerInspected, ObservedAt: response.ObservedAt.AsTime(),
		Consumer: &attachexecution.ConsumerInspection{NetworkID: response.NetworkId, ServiceID: response.ServiceId,
			CandidateSHA256: input.Source.Failure.Authority.ConsumerCandidateSHA256, HelperStopped: true, Detached: response.Detached}}
	return report, report.Validate(input)
}
