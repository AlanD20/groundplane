package dnsresolver

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type platformMutationKind string

const (
	platformEnable    platformMutationKind = "enable"
	platformDisable   platformMutationKind = "disable"
	platformUpdate    platformMutationKind = "update"
	platformConfigure platformMutationKind = "configure"
)

type platformMutationRequest struct {
	Action platformMutationKind         `json:"action"`
	Config *core.CoreDNSComponentConfig `json:"config,omitempty"`
}

func (request platformMutationRequest) action() (workexecution.Action, error) {
	if (request.Action == platformConfigure) != (request.Config != nil) {
		return "", errs.New(errs.KindValidationFailed, "CoreDNS configuration request is invalid")
	}
	switch request.Action {
	case platformEnable:
		return workexecution.ActionStart, nil
	case platformDisable:
		return workexecution.ActionStop, nil
	case platformUpdate, platformConfigure:
		return workexecution.ActionUpdate, nil
	default:
		return "", errs.New(errs.KindValidationFailed, "CoreDNS mutation action is invalid")
	}
}

func (service *PlatformMutationService) platformComponent(
	ctx context.Context,
	id string,
) (keyvalue.Versioned[componentrecord.Record], error) {
	current, err := service.components.GetComponent(ctx, id)
	if err != nil {
		return current, err
	}
	if current.Record.Desired.Owner != core.ComponentOwnerPlatform ||
		current.Record.Desired.Kind != core.ComponentKindCoreDNS {
		return current, errs.New(errs.KindStateConflict, "Platform mutations are supported only for CoreDNS")
	}
	return current, nil
}

func (service *PlatformMutationService) acceptPlatformMutation(ctx context.Context, id string, after []string,
	request platformMutationRequest, locator idempotency.IdempotencyLocator, intent platformComponentProtectedIntent,
) (idempotency.IdempotencyResponse, error) {
	if _, err := service.platformComponent(ctx, id); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	action, err := request.action()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	at := service.now().UTC()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
		Action: action, TargetID: id, TargetName: "CoreDNS", After: append([]string(nil), after...),
		CreatedAt: at, UpdatedAt: at, Deadline: at.Add(acceptedaction.QueueTimeout)}
	return acceptedaction.Submit(ctx, service.submitter, acceptedaction.Submission{
		Task: task, Kind: "platform-resolver-mutation", Locator: locator, Intent: intent.durable, Evidence: intent.protected}, request)
}

func (service *PlatformMutationService) AcceptedPreparations() []taskdependencies.Registration {
	result := make([]taskdependencies.Registration, 0, 3)
	for _, action := range []workexecution.Action{workexecution.ActionStart, workexecution.ActionStop, workexecution.ActionUpdate} {
		result = append(
			result,
			acceptedaction.Registration(
				service.submitter,
				"platform-resolver-mutation",
				action,
				ids.KindComponent,
				service.prepareAcceptedPlatformMutation,
			),
		)
	}
	return result
}

func (service *PlatformMutationService) prepareAcceptedPlatformMutation(
	ctx context.Context,
	accepted workqueue.TaskSnapshot,
	_ int64,
	at time.Time,
	request platformMutationRequest,
) (taskdependencies.Prepared, error) {
	action, err := request.action()
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if action != accepted.Task.Action {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS request differs from its accepted Task",
		)
	}
	current, err := service.platformComponent(ctx, accepted.Task.TargetID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	desired, err := componentrecord.ProjectRecord(current.Record)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	ensure := false
	if request.Action == platformConfigure {
		desired = platformComponentConfigCandidate(desired, *request.Config)
		ensure = desired.Enabled &&
			(len(current.Record.Runtime.GeneratedServices) == 0 || !current.Record.Runtime.Healthy)
	} else {
		desired, ensure, _, err = platformComponentLifecycleCandidate(desired, string(request.Action))
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
	}
	if err := ValidateComponent(service.renderer, desired); err != nil {
		return taskdependencies.Prepared{}, err
	}
	prepared, err := service.prepareAcceptedMutation(ctx, current, desired, accepted.Task, ensure, at)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	captured, item, err := resolverwork.NewWork(prepared.Input, prepared.Capture)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	task := workexecution.CloneTask(accepted.Task)
	task.Intent, task.ExpectedItems, task.UpdatedAt = captured.Intent, 1, at
	authority, err := service.tasks.PrepareResolverAuthority(ctx, prepared.Input, prepared.Capture)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: authority}, nil
}
