package attachwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Reports struct {
	diagnostics *executiondiagnostics.Repository
	queue       *workqueue.Repository
	inputs      *Inputs
	facts       *attachments.FactService
	units       *blueprintunits.Repository
}

func NewReports(
	queue *workqueue.Repository,
	inputs *Inputs,
	facts *attachments.FactService,
	units *blueprintunits.Repository,
	diagnostics *executiondiagnostics.Repository,
) (*Reports, error) {
	if queue == nil || inputs == nil || facts == nil || units == nil || diagnostics == nil {
		return nil, errs.New(errs.KindInternal, "Attach reporting requires execution storage and captured inputs")
	}
	return &Reports{queue: queue, inputs: inputs, facts: facts, units: units, diagnostics: diagnostics}, nil
}

func (service *Reports) readView(ctx context.Context, authorization agentregistration.WorkAuthorization,
	identity *agentpb.WorkAttemptIdentity,
) (workqueue.ResumeView, []keyvalue.Condition, error) {
	return service.readDirectionalView(ctx, authorization, identity, workexecution.Forward)
}

func (service *Reports) readDirectionalView(ctx context.Context, authorization agentregistration.WorkAuthorization,
	identity *agentpb.WorkAttemptIdentity, direction workexecution.Direction,
) (workqueue.ResumeView, []keyvalue.Condition, error) {
	guards, err := authorization.Conditions()
	if err != nil {
		return workqueue.ResumeView{}, nil, err
	}
	claimed, err := workexecution.DecodeAgentAttempt(identity, authorization.Identity())
	if err != nil {
		return workqueue.ResumeView{}, nil, err
	}
	view, err := authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return workqueue.ResumeView{}, nil, err
	}
	if view.Work.Item.Item.Operation != workexecution.Attach ||
		!workexecution.SameAssignment(claimed, view.Attempt.Attempt) ||
		view.Attempt.Attempt.Direction != direction ||
		view.Work.Task.Task.CollectionStartedAt != nil ||
		!view.Work.Task.Task.PublicationComplete {
		return workqueue.ResumeView{}, nil, errs.New(
			errs.KindStateConflict,
			"Attach report has no retained matching assignment",
		)
	}
	return view, guards, nil
}

func (service *Reports) CheckpointAttachProvision(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.AttachProvisionCheckpointRequest,
) (*agentpb.AttachProvisionCheckpointAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		len(request.Report) == 0 || len(request.Report) > attachexecution.MaximumProvisionReportBytes {
		return nil, errs.New(errs.KindValidationFailed, "Attach provisioning checkpoint is missing or oversized")
	}
	view, guards, err := service.readView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.StartedAt == nil {
		return nil, errs.New(errs.KindStateConflict, "Attach provisioning checkpoint has no saved execution start")
	}
	input, err := service.inputs.Forward(ctx, view)
	if err != nil {
		return nil, err
	}
	defer attachexecution.ClearProvision(input.Provision)
	report, err := attachexecution.DecodeProvisionCheckpointRequest(input, request)
	if err != nil {
		return nil, err
	}
	if report.Progress.UpdatedAt.After(time.Now().UTC()) {
		return nil, errs.New(errs.KindValidationFailed, "Attach provisioning report time is in the future")
	}
	change, err := service.inputs.repository.PrepareCheckpoint(ctx, view, input, report)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "Attach provisioning ownership changed; resend the saved report")
	}
	service.reportProgress(ctx, view, input, report.Progress)
	return attachexecution.EncodeProvisionCheckpointAck(input, report)
}
