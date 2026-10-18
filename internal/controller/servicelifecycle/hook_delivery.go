package servicelifecycle

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type LifecycleHookResolver interface {
	ResolveCapturedLifecycleHook(context.Context, servicelifecyclework.Input, *taskconfiguration.BackingHookEncryptedInputs, attachplan.HookInputConsumer) error
}

func (service *WorkReports) DeliverLifecycleHook(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ServiceLifecycleHookRequest,
) (_ *agentpb.ServiceLifecycleHookDelivery, resultErr error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || request.StageId == "" {
		return nil, errs.New(errs.KindValidationFailed, "Service lifecycle hook request is invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	captured, envelope, inputGuards, err := service.records.LoadHookInputs(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	if envelope != nil {
		defer clear(envelope.Ciphertext)
	}
	input, err := BuildWorkInput(captured)
	if err != nil {
		return nil, err
	}
	proof, err := service.records.PrepareHookDelivery(ctx, view, input, request.StageId, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	defer keyvalue.ZeroMutationBytes(proof.Mutations)
	delivery := &agentpb.ServiceLifecycleHookDelivery{Attempt: proto.CloneOf(request.Attempt), StageId: request.StageId}
	defer func() {
		if resultErr != nil && delivery.Hook != nil {
			for _, value := range delivery.Hook.Inputs {
				clear(value.Value)
			}
		}
	}()
	err = service.hooks.ResolveCapturedLifecycleHook(ctx, captured, envelope, func(values backinghook.Input) error {
		delivery.Hook = &agentpb.BackingHookProcedure{Event: input.Hook.Event,
			Command: append([]string(nil), input.Hook.Command...), TimeoutSeconds: input.Hook.TimeoutSeconds,
			BackingServiceId: captured.ServiceID}
		for _, value := range values.Values {
			delivery.Hook.Inputs = append(delivery.Hook.Inputs, &agentpb.BackingHookValue{Key: value.Key, Value: append([]byte(nil), value.Value...)})
		}
		return executionplan.ValidateLifecycleHookDelivery(view.Attempt.Attempt, input, delivery)
	})
	if err != nil {
		return nil, err
	}
	if !time.Now().Before(view.Attempt.Attempt.Deadline) {
		return nil, errs.New(errs.KindStateConflict, "Service lifecycle hook execution deadline expired")
	}
	guards = append(guards, inputGuards...)
	result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "Service lifecycle hook ownership changed before input delivery")
	}
	return delivery, nil
}
