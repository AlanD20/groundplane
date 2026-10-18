package workdispatch

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// MemberReports is shared transport for captured container-member execution.
// Operation identity in storage, not the request's action, selects settlement.
type MemberReports interface {
	CheckpointLifecycle(context.Context, agentregistration.WorkAuthorization, *agentpb.ServiceLifecycleCheckpointRequest) (*agentpb.ServiceLifecycleCheckpointAck, error)
	CompleteLifecycle(context.Context, agentregistration.WorkAuthorization, *agentpb.ServiceLifecycleCompletionRequest) (*agentpb.ServiceLifecycleCompletionAck, error)
	FailLifecycle(context.Context, agentregistration.WorkAuthorization, *agentpb.ServiceLifecycleFailureRequest) (*agentpb.ServiceLifecycleFailureAck, error)
}

type LifecycleReports interface {
	MemberReports
	RecoverLifecycle(context.Context, agentregistration.WorkAuthorization, *agentpb.ServiceLifecycleRecoveryReportRequest) (*agentpb.ServiceLifecycleRecoveryReportAck, error)
	DeliverLifecycleHook(context.Context, agentregistration.WorkAuthorization, *agentpb.ServiceLifecycleHookRequest) (*agentpb.ServiceLifecycleHookDelivery, error)
}

type LifecycleReportRouter struct {
	queue     *workqueue.Repository
	lifecycle LifecycleReports
	removal   MemberReports
}

func NewLifecycleReportRouter(queue *workqueue.Repository, lifecycle LifecycleReports, removal MemberReports) (*LifecycleReportRouter, error) {
	if queue == nil || lifecycle == nil || removal == nil {
		return nil, errs.New(errs.KindInternal, "Container-member reporting requires lifecycle and removal owners")
	}
	return &LifecycleReportRouter{queue: queue, lifecycle: lifecycle, removal: removal}, nil
}

func (router *LifecycleReportRouter) owner(ctx context.Context, authorization agentregistration.WorkAuthorization, identity *agentpb.WorkAttemptIdentity) (MemberReports, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindValidationFailed, "Container-member report context is missing")
	}
	if _, err := authorization.Conditions(); err != nil {
		return nil, err
	}
	claimed, err := workexecution.DecodeAgentAttempt(identity, authorization.Identity())
	if err != nil {
		return nil, err
	}
	view, err := authorization.ReadResult(ctx, router.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(claimed, view.Attempt.Attempt) || claimed.Direction != workexecution.Forward {
		return nil, errs.New(errs.KindStateConflict, "Container-member report differs from its retained forward assignment")
	}
	switch view.Work.Item.Item.Operation {
	case workexecution.ServiceLifecycle:
		return router.lifecycle, nil
	case workexecution.ServiceRemoval:
		return router.removal, nil
	default:
		return nil, errs.New(errs.KindStateConflict, "Operation does not accept container-member reports")
	}
}

func (router *LifecycleReportRouter) CheckpointLifecycle(ctx context.Context, authorization agentregistration.WorkAuthorization, request *agentpb.ServiceLifecycleCheckpointRequest) (*agentpb.ServiceLifecycleCheckpointAck, error) {
	owner, err := router.owner(ctx, authorization, request.GetAttempt())
	if err != nil {
		return nil, err
	}
	return owner.CheckpointLifecycle(ctx, authorization, request)
}

func (router *LifecycleReportRouter) CompleteLifecycle(ctx context.Context, authorization agentregistration.WorkAuthorization, request *agentpb.ServiceLifecycleCompletionRequest) (*agentpb.ServiceLifecycleCompletionAck, error) {
	owner, err := router.owner(ctx, authorization, request.GetAttempt())
	if err != nil {
		return nil, err
	}
	return owner.CompleteLifecycle(ctx, authorization, request)
}

func (router *LifecycleReportRouter) FailLifecycle(ctx context.Context, authorization agentregistration.WorkAuthorization, request *agentpb.ServiceLifecycleFailureRequest) (*agentpb.ServiceLifecycleFailureAck, error) {
	owner, err := router.owner(ctx, authorization, request.GetAttempt())
	if err != nil {
		return nil, err
	}
	return owner.FailLifecycle(ctx, authorization, request)
}

// Hook delivery and lifecycle restoration have their own operation-specific
// admission. Removal never reaches them through a member report fallback.
func (router *LifecycleReportRouter) RecoverLifecycle(ctx context.Context, authorization agentregistration.WorkAuthorization, request *agentpb.ServiceLifecycleRecoveryReportRequest) (*agentpb.ServiceLifecycleRecoveryReportAck, error) {
	return router.lifecycle.RecoverLifecycle(ctx, authorization, request)
}

func (router *LifecycleReportRouter) DeliverLifecycleHook(ctx context.Context, authorization agentregistration.WorkAuthorization, request *agentpb.ServiceLifecycleHookRequest) (*agentpb.ServiceLifecycleHookDelivery, error) {
	return router.lifecycle.DeliverLifecycleHook(ctx, authorization, request)
}
