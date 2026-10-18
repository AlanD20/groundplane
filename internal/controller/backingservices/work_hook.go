package backingservices

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingprovisionwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type CreationHookResolver interface {
	ResolveCapturedCreationHook(context.Context, backingprovisionwork.Input, *taskconfiguration.BackingHookEncryptedInputs, attachplan.HookInputConsumer) error
}

func (service *WorkReports) DeliverBackingProvisionHook(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.BackingProvisionHookRequest,
) (_ *agentpb.BackingProvisionHookDelivery, resultErr error) {
	if request == nil || executionplan.RejectUnknown(request) != nil {
		return nil, invalidBackingDelivery()
	}
	selection, err := service.activeDelivery(ctx, authorization, request.Attempt, request.StageId, backingprovision.AfterStart)
	if err != nil {
		return nil, err
	}
	captured, envelope, guards, err := service.records.LoadHookInputs(ctx, selection.view.Work)
	if err != nil {
		return nil, err
	}
	defer clear(captured.Wire)
	if envelope != nil {
		defer clear(envelope.Ciphertext)
	}
	delivery := &agentpb.BackingProvisionHookDelivery{Attempt: proto.CloneOf(request.Attempt), StageId: request.StageId}
	defer func() {
		if resultErr != nil && delivery.Hook != nil {
			for _, value := range delivery.Hook.Inputs {
				clear(value.Value)
			}
		}
	}()
	hook := selection.input.Assignment.Procedure.AfterStart
	err = service.hooks.ResolveCapturedCreationHook(ctx, captured, envelope, func(input backinghook.Input) error {
		delivery.Hook = &agentpb.BackingHookProcedure{Event: agentpb.BackingHookEvent_BACKING_HOOK_EVENT_AFTER_START,
			BackingServiceId: selection.input.Layout.ServiceID, Command: slices.Clone(hook.Command), TimeoutSeconds: hook.TimeoutSeconds}
		for _, value := range input.Values {
			delivery.Hook.Inputs = append(delivery.Hook.Inputs, &agentpb.BackingHookValue{Key: value.Key, Value: slices.Clone(value.Value)})
		}
		return backingprovision.ValidateHookDelivery(selection.input, delivery)
	})
	if err != nil {
		return nil, err
	}
	selection.guards = append(selection.guards, guards...)
	if err := service.grantDelivery(ctx, selection); err != nil {
		return nil, err
	}
	return delivery, nil
}
