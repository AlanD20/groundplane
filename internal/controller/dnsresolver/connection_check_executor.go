package dnsresolver

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type dnsCheckAssignments interface {
	GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
}
type dnsCheckExchange interface {
	CheckDNSConnections(context.Context, string, *agentpb.CheckDNSConnections) (*agentpb.DNSConnectionResult, error)
}

type ConnectionCheckExecutor struct {
	records *dnschecks.Repository
	tasks   dnsCheckAssignments
	channel dnsCheckExchange
}

func NewConnectionCheckExecutor(
	records *dnschecks.Repository,
	tasks dnsCheckAssignments,
	channel dnsCheckExchange,
) (*ConnectionCheckExecutor, error) {
	if records == nil || tasks == nil || channel == nil {
		return nil, errs.New(errs.KindInternal, "DNS check records, Task assignments and Agent channel are required")
	}
	return &ConnectionCheckExecutor{records: records, tasks: tasks, channel: channel}, nil
}

func (executor *ConnectionCheckExecutor) Execute(ctx context.Context, task etcd.TaskRecord) error {
	if ctx == nil || etcd.ValidateTaskRecord(task) != nil || task.Executor != taskjournal.TaskExecutorController ||
		task.Type != taskjournal.TaskCheck || task.Status != taskjournal.TaskStatusRunning ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceComponent {
		return errs.New(errs.KindValidationFailed, "DNS check Task is invalid")
	}
	loaded, err := executor.records.Load(ctx, task.ID)
	if err != nil {
		return err
	}
	record := loaded.Record
	hash, err := dnschecks.PlanHash(record)
	if err != nil || record.Owner.TaskID != task.ID || record.Owner.PlanID != task.PlanID ||
		record.Owner.OperationID != task.OperationID ||
		record.Source.ComponentID != task.Target ||
		hash != task.PlanHash ||
		record.ConfigurationSHA256 != task.Params[taskjournal.TaskDNSCheckInputParam] {
		return errs.New(errs.KindStateConflict, "DNS check differs from its captured Task input")
	}
	if record.Phase == dnschecks.Finished {
		return dnsCheckOutcomeError(record)
	}
	claim, err := executor.tasks.GetTaskAssignment(ctx, task.ID)
	if err != nil {
		return err
	}
	claimed := claim.Task.Record
	if claimed.PlanID != task.PlanID || claimed.PlanHash != task.PlanHash || claimed.OperationID != task.OperationID ||
		claimed.Target != task.Target {
		return errs.New(errs.KindStateConflict, "DNS check Task claim changed")
	}
	journal, err := dnschecks.NewJournal(executor.records, record.Owner, claim.Assignment.Record.AssignmentID)
	if err != nil {
		return err
	}
	if record.Phase == dnschecks.Checking {
		return stopDNSCheck(
			ctx,
			journal,
			"interrupted",
			errs.New(
				errs.KindStateConflict,
				"DNS check was interrupted; submit a new check instead of replaying its queries",
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return stopDNSCheck(ctx, journal, dnsCheckExchangeFailure(err), err)
	}
	if !time.Now().UTC().Before(claim.Assignment.Record.Deadline) {
		return stopDNSCheck(ctx, journal, "timeout", context.DeadlineExceeded)
	}
	request, err := record.Input()
	if err != nil {
		return err
	}
	if err := journal.Start(ctx, time.Now().UTC()); err != nil {
		return err
	}
	result, err := executor.channel.CheckDNSConnections(ctx, record.Source.AgentID, request)
	if err != nil {
		return stopDNSCheck(ctx, journal, dnsCheckExchangeFailure(err), err)
	}
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := journal.Finish(settlement, result, time.Now().UTC()); err != nil {
		return err
	}
	completed, err := executor.records.Load(settlement, task.ID)
	if err != nil {
		return err
	}
	return dnsCheckOutcomeError(completed.Record)
}

func stopDNSCheck(ctx context.Context, journal *dnschecks.Journal, failure string, cause error) error {
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := journal.Stop(settlement, failure, time.Now().UTC()); err != nil {
		return err
	}
	return cause
}

func dnsCheckExchangeFailure(err error) string {
	kind, _ := errs.KindOf(err)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case kind == errs.KindStorageUnavailable:
		return "agent_unavailable"
	default:
		return "execution_failed"
	}
}

func dnsCheckOutcomeError(record dnschecks.Record) error {
	if record.Outcome == "passed" {
		return nil
	}
	message := "DNS check failed; inspect its per-server query results"
	switch record.Failure {
	case "configuration_changed":
		message = "CoreDNS configuration changed or was not serving the captured files; wait for its update and submit a new check"
	case "query_selection_invalid":
		message = "DNS query name does not select its configured forwarder; choose a name inside that domain"
	case "runtime_unavailable", "agent_unavailable":
		message = "CoreDNS or its Agent is unavailable; inspect their runtime before checking DNS again"
	case "interrupted", "canceled", "journal":
		message = "DNS check was interrupted; submit a new check against the current configuration"
	case "timeout":
		message = "DNS check exceeded its deadline; inspect resolver and upstream connectivity"
	}
	return errs.New(errs.KindStateConflict, message)
}
