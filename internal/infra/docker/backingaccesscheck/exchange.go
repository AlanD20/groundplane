package backingaccesscheck

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Exchange owns the fixed probe/cleanup procedure, not the Engine's lifetime.
// Each request carries its Task's pinned helper image and consumer authority.
type Exchange struct{ engine Engine }

func NewExchange(engine Engine) (*Exchange, error) {
	if engine == nil {
		return nil, errs.New(errs.KindInternal, "Backing access exchange engine is required")
	}
	return &Exchange{engine: engine}, nil
}

func (exchange *Exchange) Check(
	ctx context.Context,
	request *agentpb.CheckBackingAccess,
) (*agentpb.BackingAccessResult, error) {
	if ctx == nil || exchange == nil || exchange.engine == nil {
		return nil, errs.New(errs.KindInternal, "Backing access exchange is not configured")
	}
	if err := backingaccess.ValidateMachineRequest(request); err != nil {
		return nil, err
	}
	executor, err := New(exchange.engine, request.HelperImage)
	if err != nil {
		return nil, err
	}
	parameters := backingaccess.ParametersFromMachine(request.Parameters)
	response := &agentpb.BackingAccessResult{RequestId: request.RequestId, TaskId: request.TaskId,
		HelperName: backingaccess.HelperName(request.TaskId)}
	if request.CleanupOnly {
		err := executor.Cleanup(ctx, request.TaskId, request.ConsumerContainerId, parameters)
		response.CleanupRequired, response.Failure = err != nil, backingaccess.MachineFailure(err)
		return response, nil
	}
	executed, err := executor.Execute(
		ctx,
		request.TaskId,
		request.ConsumerContainerId,
		request.ConsumerRuntime,
		parameters,
	)
	response.CleanupRequired, response.Failure = executed.CleanupRequired, backingaccess.MachineFailure(err)
	if executed.Result != nil {
		response.Observation = backingaccess.ObservationForMachine(*executed.Result)
	}
	if executed.HelperName == "" {
		// An early failure before Create still proves absence through the exact
		// deterministic operation name. It cannot certify a forgotten helper.
		cleanupErr := executor.Cleanup(
			context.WithoutCancel(ctx),
			request.TaskId,
			request.ConsumerContainerId,
			parameters,
		)
		response.CleanupRequired = cleanupErr != nil
		response.Failure = backingaccess.MachineFailure(errors.Join(err, cleanupErr))
	}
	if response.CleanupRequired {
		response.Failure = backingaccess.CleanupFailure
	}
	return response, backingaccess.ValidateMachineResult(request, response)
}
