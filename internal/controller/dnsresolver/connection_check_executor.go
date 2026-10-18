package dnsresolver

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type dnsCheckExchange interface {
	CheckDNSConnections(context.Context, string, *agentpb.CheckDNSConnections) (*agentpb.DNSConnectionResult, error)
}

type ConnectionCheckExecutor struct {
	records *dnschecks.Repository
	channel dnsCheckExchange
}

func NewConnectionCheckExecutor(
	records *dnschecks.Repository,
	channel dnsCheckExchange,
) (*ConnectionCheckExecutor, error) {
	if records == nil || channel == nil {
		return nil, errs.New(errs.KindInternal, "DNS check records and Agent channel are required")
	}
	return &ConnectionCheckExecutor{records: records, channel: channel}, nil
}

func (executor *ConnectionCheckExecutor) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	err := executor.execute(ctx, delivery)
	if err == nil || ctx == nil {
		return err
	}
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	loaded, loadErr := executor.loadDelivery(settlement, delivery)
	if loadErr != nil {
		return errs.WrapJoined(errs.KindStateConflict, err, loadErr)
	}
	if loaded.Record.Phase == dnschecks.Finished {
		return err
	}
	attempt := delivery.Attempt.Attempt
	journal, journalErr := dnschecks.NewJournal(executor.records, loaded.Record.Owner, attempt.ID, attempt.Executor)
	if journalErr != nil {
		return errs.WrapJoined(errs.KindStateConflict, err, journalErr)
	}
	if stopErr := journal.Stop(settlement, dnsCheckExchangeFailure(err), time.Now().UTC()); stopErr != nil {
		return errs.WrapJoined(errs.KindStateConflict, err, stopErr)
	}
	return err
}

func (executor *ConnectionCheckExecutor) execute(ctx context.Context, delivery workqueue.Delivery) error {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if ctx == nil || item.Validate() != nil || attempt.Validate() != nil ||
		item.Operation != workexecution.DNSCheck || item.Phase != workexecution.Running || item.ActiveAttemptID != attempt.ID ||
		attempt.Executor.Executor != workexecution.Controller || attempt.Direction != workexecution.Forward ||
		attempt.TaskID != item.TaskID || attempt.WorkItemID != item.ID || attempt.StartedAt == nil || attempt.FinishedAt != nil ||
		item.Input == nil || *item.Input != attempt.Input {
		return errs.New(errs.KindValidationFailed, "DNS check Task is invalid")
	}
	loaded, err := executor.records.Load(ctx, item.TaskID)
	if err != nil {
		return err
	}
	record := loaded.Record
	hash, err := dnschecks.PlanHash(record)
	if err != nil || record.Owner.TaskID != item.TaskID || record.Owner.WorkItemID != item.ID || record.Owner.PlanID != attempt.Input.ID ||
		record.Source.ComponentID != item.TargetID || hash != attempt.Input.SHA256 {
		return errs.New(errs.KindStateConflict, "DNS check differs from its captured Task input")
	}
	if record.Phase == dnschecks.Finished {
		return dnsCheckOutcomeError(record)
	}
	journal, err := dnschecks.NewJournal(executor.records, record.Owner, attempt.ID, attempt.Executor)
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
	if !time.Now().UTC().Before(attempt.Deadline) {
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
	completed, err := executor.records.Load(settlement, item.TaskID)
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
