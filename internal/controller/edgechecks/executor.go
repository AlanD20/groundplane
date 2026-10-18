// Package edgechecks coordinates the captured connector and public Route check.
// Each probe starts once under its durable claim; interrupted effects resume
// cleanup only, never an automatic replay against a later runtime.
package edgechecks

import (
	"bytes"
	"context"
	"time"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/edgeconnectioncheck"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type exchange interface {
	CheckEdgeConnections(context.Context, string, *agentpb.CheckEdgeConnections) (*agentpb.EdgeConnectionResult, error)
}

type Executor struct {
	repository *checkrecord.Repository
	exchange   exchange
}

func NewExecutor(repository *checkrecord.Repository, channel exchange) (*Executor, error) {
	if repository == nil || channel == nil {
		return nil, errs.New(errs.KindInternal, "Tunnel check repository and Agent channel are required")
	}
	return &Executor{repository: repository, exchange: channel}, nil
}

func (executor *Executor) execute(ctx context.Context, delivery workqueue.Delivery) error {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if ctx == nil || item.Validate() != nil || attempt.Validate() != nil ||
		item.Operation != workexecution.EdgeCheck || item.Phase != workexecution.Running ||
		attempt.Executor.Executor != workexecution.Controller || attempt.Direction != workexecution.Forward ||
		attempt.StartedAt == nil || attempt.FinishedAt != nil || item.ActiveAttemptID != attempt.ID ||
		attempt.TaskID != item.TaskID || attempt.WorkItemID != item.ID ||
		item.Input == nil || *item.Input != attempt.Input {
		return errs.New(errs.KindValidationFailed, "Tunnel check execution is invalid")
	}
	loaded, err := executor.repository.Load(ctx, item.TaskID)
	if err != nil {
		return err
	}
	record := loaded.Record
	planHash, err := checkrecord.PlanHash(record)
	if err != nil || record.Owner.TaskID != item.TaskID || record.Owner.WorkItemID != item.ID ||
		record.Owner.PlanID != attempt.Input.ID || planHash != attempt.Input.SHA256 ||
		record.Source.Request.TargetID() != item.TargetID {
		return errs.New(errs.KindStateConflict, "Tunnel check differs from its captured execution input")
	}
	if record.Phase == checkrecord.Finished {
		return outcomeError(record)
	}
	journal, err := checkrecord.NewJournal(executor.repository, record.Owner, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(ctx, attempt.Deadline)
	defer cancel()
	switch record.Phase {
	case checkrecord.PrivateChecking, checkrecord.Cleaning:
		return executor.cleanup(ctx, record, journal)
	case checkrecord.PublicChecking:
		return executor.public(ctx, record, journal)
	case checkrecord.Ready:
	default:
		return errs.New(errs.KindStateConflict, "Tunnel check is no longer owned by this execution")
	}
	if err := ctx.Err(); err != nil {
		return stop(ctx, journal, err)
	}
	request, err := requestFor(record, false)
	if err != nil {
		return stop(ctx, journal, err)
	}
	if err := journal.Start(ctx, time.Now().UTC()); err != nil {
		return err
	}
	result, err := executor.exchange.CheckEdgeConnections(ctx, record.Source.AgentID, request)
	if err != nil {
		return err
	} // possible helper effects remain owned
	// The transport assigns a fresh exchange ID. All durable identities still
	// match the original captured request, including the cleanup-only flag.
	request.RequestId = result.GetRequestId()
	if err := contract.ValidateMachineResult(request, result); err != nil {
		return err
	}
	var observed *contract.PrivateResult
	if len(result.PrivateResultJson) != 0 {
		observation, err := contract.ReadPrivateResult(bytes.NewReader(result.PrivateResultJson), record.Source.Request)
		if err != nil {
			return err
		}
		observed = &observation
	}
	settlement, settleCancel := settlementContext(ctx)
	defer settleCancel()
	if err := journal.ObservePrivate(settlement, observed, result.CleanupRequired, result.Failure); err != nil {
		return err
	}
	latest, err := executor.repository.Load(settlement, item.TaskID)
	if err != nil {
		return err
	}
	if latest.Record.CleanupPending {
		return errs.New(
			errs.KindStateConflict,
			"Tunnel check helper cleanup is incomplete; retry cleanup before removing resources",
		)
	}
	return executor.public(ctx, latest.Record, journal)
}

func requestFor(record checkrecord.Record, cleanup bool) (*agentpb.CheckEdgeConnections, error) {
	runtime, action, err := record.Source.CapturedRuntime()
	if err != nil {
		return nil, err
	}
	parameters, err := contract.MarshalRequest(record.Source.Request)
	if err != nil {
		return nil, err
	}
	return &agentpb.CheckEdgeConnections{TaskId: record.HelperTaskID, HelperImage: record.Source.HelperImage,
		ConnectorContainerId: record.Source.ConnectorContainerID, ParametersJson: parameters,
		Runtime: runtime, Action: action, CleanupOnly: cleanup}, nil
}

func (executor *Executor) public(ctx context.Context, record checkrecord.Record, journal *checkrecord.Journal) error {
	if record.Public != nil || record.PublicFailure != "" {
		return executor.finish(ctx, record, journal)
	}
	if record.PublicStartedAt != nil {
		// A started public probe with no stored outcome is interrupted, not
		// permission to query again. There are no helper effects left to clean.
		settlement, cancel := settlementContext(ctx)
		defer cancel()
		if err := journal.Stop(settlement, "interrupted", time.Now().UTC()); err != nil {
			return err
		}
		return errs.New(errs.KindStateConflict, "public Route check was interrupted; start a new check")
	}
	if err := ctx.Err(); err != nil {
		return stop(ctx, journal, err)
	}
	if err := journal.StartPublic(ctx, time.Now().UTC()); err != nil {
		return err
	}
	result, probeErr := edgeconnectioncheck.CheckPublic(ctx, record.Source.Request.Target)
	var observed *contract.PublicResult
	if probeErr == nil {
		observed = &result
	}
	settlement, cancel := settlementContext(ctx)
	defer cancel()
	if err := journal.ObservePublic(settlement, observed, contract.MachineFailure(probeErr)); err != nil {
		return err
	}
	latest, err := executor.repository.Load(settlement, record.Owner.TaskID)
	if err != nil {
		return err
	}
	return executor.finish(settlement, latest.Record, journal)
}

func (executor *Executor) finish(ctx context.Context, record checkrecord.Record, journal *checkrecord.Journal) error {
	settlement, cancel := settlementContext(ctx)
	defer cancel()
	if err := journal.Finish(settlement, time.Now().UTC()); err != nil {
		return err
	}
	latest, err := executor.repository.Load(settlement, record.Owner.TaskID)
	if err != nil {
		return err
	}
	return outcomeError(latest.Record)
}

func (executor *Executor) cleanup(ctx context.Context, record checkrecord.Record, journal *checkrecord.Journal) error {
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), contract.ExchangeTimeout+15*time.Second)
	defer cancel()
	if err := journal.ResumeCleanup(settlement); err != nil {
		return err
	}
	request, err := requestFor(record, true)
	if err != nil {
		return err
	}
	result, err := executor.exchange.CheckEdgeConnections(settlement, record.Source.AgentID, request)
	if err != nil {
		return err
	}
	request.RequestId = result.GetRequestId()
	if contract.ValidateMachineResult(request, result) != nil || result.CleanupRequired || result.Failure != "" {
		return errs.New(errs.KindStateConflict, "Tunnel helper cleanup is not proven; its ownership is retained")
	}
	if err := journal.FinishCleanup(settlement, time.Now().UTC()); err != nil {
		return err
	}
	latest, err := executor.repository.Load(settlement, record.Owner.TaskID)
	if err != nil {
		return err
	}
	return outcomeError(latest.Record)
}

func stop(ctx context.Context, journal *checkrecord.Journal, cause error) error {
	settlement, cancel := settlementContext(ctx)
	defer cancel()
	if err := journal.Stop(settlement, contract.MachineFailure(cause), time.Now().UTC()); err != nil {
		return err
	}
	return cause
}

func settlementContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
}

func outcomeError(record checkrecord.Record) error {
	if record.Outcome == "passed" || record.Outcome == "protected" {
		return nil
	}
	switch record.Failure {
	case string(contract.TimeoutFailure):
		return context.DeadlineExceeded
	case string(contract.Canceled):
		return context.Canceled
	case string(contract.NotReady):
		return errs.New(errs.KindStateConflict, "Tunnel connector is not ready; inspect its runtime and logs")
	case string(contract.ConnectionFailure):
		return errs.New(
			errs.KindStorageUnavailable,
			"Tunnel or Route endpoint could not be reached; inspect the failed stage",
		)
	case string(contract.DNSFailure):
		return errs.New(errs.KindStorageUnavailable, "public Route hostname did not resolve; check its DNS records")
	case string(contract.UnsafeAddress):
		return errs.New(errs.KindValidationFailed, "public Route hostname resolves to a non-public address")
	case string(contract.TLSFailure):
		return errs.New(
			errs.KindRequestFailed,
			"public Route TLS verification failed; check the hostname and certificate",
		)
	case string(contract.HTTPFailure):
		return errs.New(
			errs.KindRequestFailed,
			"Route returned an unsuccessful HTTP response; inspect the recorded status",
		)
	case contract.ConfigurationChanged:
		return errs.New(errs.KindStateConflict, "captured Tunnel runtime changed; start a new check")
	case contract.AgentUnavailable:
		return errs.New(errs.KindStorageUnavailable, "captured Agent could not execute the Tunnel check")
	case contract.CleanupFailure:
		return errs.New(errs.KindStateConflict, "Tunnel helper cleanup failed; retry cleanup before removing resources")
	case "interrupted":
		return errs.New(
			errs.KindStateConflict,
			"Tunnel check was interrupted; cleanup completed but connectivity was not certified",
		)
	default:
		return errs.New(
			errs.KindRequestFailed,
			"Tunnel check could not produce a complete result; inspect its recorded stages",
		)
	}
}
