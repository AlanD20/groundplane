package backingservices

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingprovisionwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type WorkReports struct {
	queue       *workqueue.Repository
	records     *backingprovisionwork.Repository
	files       CapturedFileReader
	hooks       CreationHookResolver
	diagnostics *executiondiagnostics.Repository
}

func NewWorkReports(queue *workqueue.Repository, records *backingprovisionwork.Repository, files CapturedFileReader, hooks CreationHookResolver,
	diagnostics *executiondiagnostics.Repository,
) (*WorkReports, error) {
	if queue == nil || records == nil || files == nil || hooks == nil || diagnostics == nil {
		return nil, errs.New(errs.KindInternal, "backing creation reporting requires native execution and diagnostic storage")
	}
	return &WorkReports{queue: queue, records: records, files: files, hooks: hooks, diagnostics: diagnostics}, nil
}

func (service *WorkReports) CheckpointBackingProvision(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.BackingProvisionCheckpointRequest,
) (*agentpb.BackingProvisionCheckpointAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || len(request.Progress) == 0 || len(request.Progress) > backingprovision.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "backing creation checkpoint is missing or oversized")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt, true)
	if err != nil {
		return nil, err
	}
	input, _, err := service.records.Load(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	defer clear(input.Wire)
	procedure, err := input.Procedure()
	if err != nil {
		return nil, err
	}
	layout, err := backingprovision.CaptureLayout(view.Attempt.Attempt, procedure)
	if err != nil {
		return nil, err
	}
	progress, err := backingprovision.DecodeProgress(view.Attempt.Attempt, layout, request.Progress)
	if err != nil {
		return nil, err
	}
	at := time.Now().UTC()
	if progress.InFlight && (view.Checkpoint.Record == nil || progress.Sequence > view.Checkpoint.Record.Sequence) {
		captured, source, current, err := service.records.ReadAdmission(ctx, view.Work)
		clear(captured.Wire)
		if err != nil {
			return nil, err
		}
		if !current {
			return nil, errs.New(errs.KindStateConflict, "backing creation was superseded before its next execution stage")
		}
		guards = append(guards, source.Conditions...)
	}
	change, checkpoint, err := service.records.PrepareProgress(ctx, view, progress, at)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "backing creation checkpoint ownership changed; resend the saved report")
	}
	evidence, err := encodeBackingReference(checkpoint.Evidence)
	if err != nil {
		return nil, err
	}
	return &agentpb.BackingProvisionCheckpointAck{Attempt: proto.CloneOf(request.Attempt), Progress: append([]byte(nil), request.Progress...), Evidence: evidence}, nil
}

func (service *WorkReports) reportView(ctx context.Context, authorization agentregistration.WorkAuthorization,
	identity *agentpb.WorkAttemptIdentity, requireStarted bool,
) (workqueue.ResumeView, error) {
	claimed, err := workexecution.DecodeAgentAttempt(identity, authorization.Identity())
	if err != nil {
		return workqueue.ResumeView{}, err
	}
	view, err := service.queue.ReadResume(ctx, claimed.TaskID, claimed.WorkItemID, claimed.ID, authorization.Identity())
	if err != nil {
		return workqueue.ResumeView{}, err
	}
	if !workexecution.SameAssignment(claimed, view.Attempt.Attempt) || requireStarted && view.Attempt.Attempt.StartedAt == nil ||
		view.Work.Item.Item.Operation != workexecution.BackingProvision || !view.Work.Task.Task.PublicationComplete || view.Work.Task.Task.CollectionStartedAt != nil {
		return workqueue.ResumeView{}, errs.New(errs.KindStateConflict, "backing creation report has no retained captured execution")
	}
	return view, nil
}

func encodeBackingReference(reference workexecution.Input) (*agentpb.WorkInputReference, error) {
	if err := reference.Validate(); err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.WorkInputReference{InputId: reference.ID, Sha256: digest}, nil
}
