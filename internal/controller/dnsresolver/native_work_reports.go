package dnsresolver

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkReports struct {
	queue       *workqueue.Repository
	records     *resolverwork.Repository
	diagnostics *executiondiagnostics.Repository
}

func NewWorkReports(
	queue *workqueue.Repository,
	records *resolverwork.Repository,
	diagnostics *executiondiagnostics.Repository,
) (*WorkReports, error) {
	if queue == nil || records == nil || diagnostics == nil {
		return nil, errs.New(errs.KindInternal, "CoreDNS reporting requires native execution and diagnostic storage")
	}
	return &WorkReports{queue: queue, records: records, diagnostics: diagnostics}, nil
}

func (service *WorkReports) reportView(ctx context.Context, authorization agentregistration.WorkAuthorization,
	identity *agentpb.WorkAttemptIdentity, started bool,
) (workqueue.ResumeView, error) {
	claimed, err := workexecution.DecodeAgentAttempt(identity, authorization.Identity())
	if err != nil {
		return workqueue.ResumeView{}, err
	}
	view, err := authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return workqueue.ResumeView{}, err
	}
	if !workexecution.SameAssignment(claimed, view.Attempt.Attempt) ||
		started && view.Attempt.Attempt.StartedAt == nil ||
		view.Work.Item.Item.Operation != workexecution.Resolver ||
		!view.Work.Task.Task.PublicationComplete ||
		view.Work.Task.Task.CollectionStartedAt != nil {
		return workqueue.ResumeView{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS report has no retained captured execution",
		)
	}
	return view, nil
}

func (service *WorkReports) CheckpointResolver(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ResolverWorkCheckpointRequest,
) (*agentpb.ResolverWorkCheckpointAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || len(request.Progress) == 0 ||
		len(request.Progress) > resolverexecution.MaximumReportBytes {
		return nil, errs.New(errs.KindValidationFailed, "CoreDNS checkpoint is missing or oversized")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt, true)
	if err != nil {
		return nil, err
	}
	stored, err := service.records.Load(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	layout, err := resolverexecution.CaptureLayout(view.Attempt.Attempt, stored.Input)
	if err != nil {
		return nil, err
	}
	progress, err := resolverexecution.DecodeProgress(view.Attempt.Attempt, layout, request.Progress)
	if err != nil {
		return nil, err
	}
	if progress.InFlight && (view.Checkpoint.Record == nil || progress.Sequence > view.Checkpoint.Record.Sequence) {
		_, source, current, err := service.records.ReadAdmission(ctx, view.Work)
		if err != nil {
			return nil, err
		}
		if !current {
			return nil, errs.New(errs.KindStateConflict, "CoreDNS input was superseded before its next execution stage")
		}
		guards = append(guards, source.Conditions...)
	}
	change, checkpoint, err := service.records.PrepareProgress(ctx, view, progress, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	if err := service.commitReport(ctx, guards, change); err != nil {
		return nil, err
	}
	service.reportProgress(ctx, view, stored.Input, layout, progress)
	evidence, err := resolverexecution.EncodeReference(checkpoint.Evidence)
	if err != nil {
		return nil, err
	}
	return &agentpb.ResolverWorkCheckpointAck{
		Attempt:  proto.CloneOf(request.Attempt),
		Progress: append([]byte(nil), request.Progress...),
		Evidence: evidence,
	}, nil
}

func (service *WorkReports) commitReport(
	ctx context.Context,
	guards []keyvalue.Condition,
	change workqueue.Change,
) error {
	result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "CoreDNS execution ownership changed; resend the saved report")
	}
	return nil
}

func resolverResultAck(
	request *agentpb.WorkAttemptIdentity,
	attempt workexecution.Attempt,
	kind string,
) (*agentpb.ResolverWorkResultAck, error) {
	if attempt.FinishedAt == nil || attempt.Result == nil {
		return nil, errs.New(errs.KindStateConflict, "CoreDNS result has no terminal receipt")
	}
	evidence, err := resolverexecution.EncodeReference(*attempt.Result)
	if err != nil {
		return nil, err
	}
	ack := &agentpb.ResolverWorkResultAck{Attempt: proto.CloneOf(request), Kind: kind, Evidence: evidence,
		Outcome: string(attempt.Outcome), FinishedAt: timestamppb.New(*attempt.FinishedAt)}
	if attempt.StartedAt != nil {
		ack.StartedAt = timestamppb.New(*attempt.StartedAt)
	}
	return ack, nil
}
