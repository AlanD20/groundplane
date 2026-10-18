package removalruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RecoveryExchange interface {
	Exchange
	ConfigurationExchange
}

// Runtime retains host dependencies across reconnects, but binds configuration
// delivery to the execution's own session. An old execution cannot acquire the
// replacement session's transport by reading shared mutable connection state.
type Runtime struct {
	root         string
	journal      *agentworkjournal.Journal
	helper       Helper
	materializer *materialization.Runtime
	ownership    *agentownership.Lease
}

func NewRuntime(root string, journal *agentworkjournal.Journal, helper Helper, materializer *materialization.Runtime, ownership *agentownership.Lease) (*Runtime, error) {
	if environmentpath.ValidateRoot(root) != nil || journal == nil || helper == nil || materializer == nil || ownership == nil {
		return nil, errs.New(errs.KindInternal, "service removal recovery requires host dependencies and ownership")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &Runtime{root: root, journal: journal, helper: helper, materializer: materializer, ownership: ownership}, nil
}

func (runtime *Runtime) Run(ctx context.Context, input Input, exchange RecoveryExchange) (workexecution.Attempt, error) {
	configuration, err := NewRecoveryConfiguration(runtime.materializer, exchange, runtime.ownership)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	host, err := NewHost(runtime.root, runtime.journal, runtime.helper, configuration, runtime.ownership)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	executor, err := NewExecutor(runtime.journal, host)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.Run(ctx, input, exchange)
}

func (runtime *Runtime) Retire(ctx context.Context, terminal workexecution.Attempt) error {
	return runtime.journal.Retire(ctx, terminal)
}
