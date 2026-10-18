package attachruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backingadapter"
	"github.com/AlanD20/groundplane/internal/agent/backinghookruntime"
	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/internal/infra/docker/backingcommand"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// FactStore acknowledges durable, attempt-bound encrypted fact storage before
// successful hook execution can advance its command checkpoint.
type FactStore interface {
	StoreProvisionFacts(context.Context, attachexecution.ProvisionInput, backinghook.Output) error
}

type ConsumerHelper interface {
	ExecuteAttachConsumer(context.Context, *agentpb.AttachConsumerHelperRequest) (*agentpb.AttachConsumerHelperResponse, error)
}

type BackingHost struct {
	journal   *agentworkjournal.Journal
	adapter   *backingadapter.Runtime
	ownership *agentownership.Lease
	commands  *backingcommand.Executor
	consumer  ConsumerHelper
}

func NewBackingHost(journal *agentworkjournal.Journal, adapter *backingadapter.Runtime,
	ownership *agentownership.Lease, commands *backingcommand.Executor, consumer ConsumerHelper,
) (*BackingHost, error) {
	if journal == nil || adapter == nil || commands == nil || consumer == nil {
		return nil, errs.New(errs.KindInternal, "Attach provisioning host requires its journal, adapter catalog and durable command executor")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &BackingHost{journal: journal, adapter: adapter, ownership: ownership, commands: commands, consumer: consumer}, nil
}

func (host *BackingHost) ExecuteProvisionCommand(ctx context.Context, input attachexecution.ProvisionInput,
	progress attachexecution.ProvisionProgress, inspectOnly bool, factsStore FactStore,
) (attachexecution.ProvisionCommandResult, error) {
	authority, err := input.Authority()
	if err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	if ctx == nil || factsStore == nil || progress.Validate(input.Attempt, authority) != nil || !progress.InFlight {
		return attachexecution.ProvisionCommandResult{}, errs.New(errs.KindStateConflict, "Attach command requires an acknowledged in-flight boundary")
	}
	saved, err := host.journal.AttachProvision(ctx, input)
	if err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	if saved == nil || !saved.Acknowledged {
		return attachexecution.ProvisionCommandResult{}, errs.New(errs.KindStateConflict, "Attach command has no acknowledged Agent journal boundary")
	}
	want, err := (attachexecution.ProvisionReport{Progress: progress}).CheckpointAuthority(input.Attempt, authority)
	if err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	got, err := saved.Report.CheckpointAuthority(input.Attempt, authority)
	if err != nil || got.Sequence != want.Sequence || got.Evidence != want.Evidence {
		return attachexecution.ProvisionCommandResult{}, errs.New(errs.KindStateConflict, "Attach command differs from its saved authorization")
	}
	if result := saved.CommandResult; result != nil {
		return *result, nil
	}
	if authority.IsConsumerCommand(progress.Completed) {
		return host.connectConsumer(ctx, input, progress, inspectOnly)
	}
	if inspectOnly || len(saved.Executions) != 0 {
		if err := host.observeProvisionExecs(ctx, input, saved); err != nil {
			return attachexecution.ProvisionCommandResult{}, err
		}
		return attachexecution.ProvisionCommandResult{}, errs.New(errs.KindStateConflict, "Attach command was interrupted without a saved result; automatic replay is disabled")
	}
	if err := host.ownership.Check(); err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	containerID, err := host.adapter.BackingContainer(ctx, input.Provision.BackingServiceId)
	if err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	if hook := input.Provision.GetCustomAttach(); hook != nil {
		if err := host.adapter.VerifyBackingContainer(ctx, containerID, input.Provision.BackingServiceId); err != nil {
			return attachexecution.ProvisionCommandResult{}, err
		}
		definition, values, schema, err := executionplan.DecodeBackingHookProcedure(hook)
		if err != nil {
			return attachexecution.ProvisionCommandResult{}, err
		}
		recorder := commandRecorder{journal: host.journal, input: input, ownership: host.ownership}
		facts, err := backinghookruntime.ExecuteAttach(ctx, host.commands, recorder, containerID, definition, values, schema)
		if err != nil {
			return attachexecution.ProvisionCommandResult{}, err
		}
		defer facts.Clear()
		if err := factsStore.StoreProvisionFacts(ctx, input, facts); err != nil {
			return attachexecution.ProvisionCommandResult{}, err
		}
	} else {
		procedures, err := attachexecution.ProvisionProcedures(input.Provision)
		if err != nil {
			return attachexecution.ProvisionCommandResult{}, err
		}
		defer func() {
			for _, procedure := range procedures {
				clear(procedure.Password)
			}
		}()
		recorder := commandRecorder{journal: host.journal, input: input, ownership: host.ownership}
		if err := host.adapter.ExecuteCapturedProcedure(ctx, procedures[progress.Completed], containerID, host.commands, recorder); err != nil {
			return attachexecution.ProvisionCommandResult{}, err
		}
	}
	result := attachexecution.ProvisionCommandResult{Ordinal: progress.Completed, StartedAt: *progress.BeganAt, FinishedAt: time.Now().UTC()}
	// A cancelled delivery still must durably save its observed successful
	// return; this bounded write does not authorize another external effect.
	storage, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := host.journal.SaveAttachProvisionCommandResult(storage, input, result); err != nil {
		return attachexecution.ProvisionCommandResult{}, err
	}
	return result, nil
}
