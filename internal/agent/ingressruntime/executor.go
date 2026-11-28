// Package ingressruntime executes one captured Host public ingress publication.
package ingressruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Host interface {
	Apply(context.Context, ingressexecution.Plan) (ingressexecution.Report, error)
	Observe(context.Context, ingressexecution.Plan) (ingressexecution.Report, error)
	Recover(context.Context, ingressexecution.Plan) (ingressexecution.Report, error)
}

type Exchange interface {
	StartAttempt(context.Context, workexecution.Attempt) (*agentpb.WorkStartAck, error)
	ReportPublicIngress(context.Context, *agentpb.PublicIngressResultRequest) error
}

type Executor struct {
	journal *agentworkjournal.Journal
	host    Host
}

func NewExecutor(journal *agentworkjournal.Journal, host Host) (*Executor, error) {
	if journal == nil || host == nil {
		return nil, errs.New(errs.KindInternal, "public ingress requires a Host runtime and Agent journal")
	}
	return &Executor{journal: journal, host: host}, nil
}

func (executor *Executor) Run(
	ctx context.Context,
	attempt workexecution.Attempt,
	plan ingressexecution.Plan,
	exchange Exchange,
) error {
	if ctx == nil || exchange == nil {
		return errs.New(errs.KindInternal, "public ingress requires an authenticated Agent exchange")
	}
	if err := ingressexecution.ValidateAssignment(attempt, plan); err != nil {
		return err
	}
	start, err := exchange.StartAttempt(ctx, attempt)
	if err != nil {
		return err
	}
	started, reconciled, err := workexecution.DecodeAgentStartAck(start, attempt)
	if err != nil {
		return err
	}
	attempt.StartedAt = &started
	fresh, saved, err := executor.journal.BeginPublicIngress(ctx, attempt)
	if err != nil {
		return err
	}
	var report ingressexecution.Report
	if saved != nil {
		report = *saved
	} else {
		effect, cancel := context.WithDeadline(ctx, attempt.Deadline)
		action := executor.host.Apply
		switch {
		case reconciled || !fresh:
			action = executor.host.Observe
		case attempt.Direction == workexecution.Recovery:
			action = executor.host.Recover
		}
		report, err = action(effect, plan)
		cancel()
		if err != nil || !validResult(plan, report) {
			report = ingressexecution.Report{Result: ingressexecution.Uncertain}
		}
	}
	request, err := ingressexecution.EncodeReport(attempt, report)
	if err != nil {
		return err
	}
	if err := exchange.ReportPublicIngress(ctx, request); err != nil {
		return err
	}
	return executor.journal.SettleTerminalDelivery(ctx, attempt.ID)
}

func validResult(plan ingressexecution.Plan, report ingressexecution.Report) bool {
	if report.Validate() != nil {
		return false
	}
	switch report.Result {
	case ingressexecution.Applied:
		return report.Receipt.Revision == plan.Revision && report.Receipt.Digest == plan.Digest &&
			(plan.Predecessor == nil || report.Receipt.RuntimeID == plan.Predecessor.RuntimeID)
	case ingressexecution.Restored:
		return plan.Predecessor != nil && *report.Receipt == *plan.Predecessor
	default:
		return true
	}
}
