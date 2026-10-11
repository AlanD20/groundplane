package componentaction

import (
	"bytes"
	"context"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	edgecheck "github.com/AlanD20/groundplane/internal/infra/docker/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// EdgeConnectionChecker binds the catalog-authenticated target to the fixed
// Docker helper. Cleanup consumes only the original captured ownership; it
// neither requires a still-running connector nor starts a replacement probe.
type EdgeConnectionChecker struct {
	runtime *Runtime
	engine  edgecheck.Engine
}

func NewEdgeConnectionChecker(runtime *Runtime, engine edgecheck.Engine) (*EdgeConnectionChecker, error) {
	if runtime == nil || engine == nil {
		return nil, errs.New(errs.KindInternal, "Tunnel diagnostic runtime and executor are required")
	}
	return &EdgeConnectionChecker{runtime: runtime, engine: engine}, nil
}

func (checker *EdgeConnectionChecker) Check(
	ctx context.Context,
	request *agentpb.CheckEdgeConnections,
) (*agentpb.EdgeConnectionResult, error) {
	if ctx == nil || checker == nil || checker.runtime == nil || checker.engine == nil {
		return nil, errs.New(errs.KindInternal, "Tunnel diagnostic executor is unavailable")
	}
	if err := contract.ValidateMachineRequest(request); err != nil {
		return nil, err
	}
	executor, err := edgecheck.New(checker.engine, request.HelperImage)
	if err != nil {
		return nil, err
	}
	parameters, err := contract.ReadRequest(bytes.NewReader(request.ParametersJson))
	if err != nil {
		return nil, err
	}
	result := &agentpb.EdgeConnectionResult{
		RequestId: request.RequestId, TaskId: request.TaskId, ConnectorContainerId: request.ConnectorContainerId,
		HelperName: contract.HelperName(request.TaskId), CleanupOnly: request.CleanupOnly,
	}
	if request.CleanupOnly {
		err := executor.Cleanup(ctx, request.TaskId, request.ConnectorContainerId, parameters)
		result.CleanupRequired, result.Failure = err != nil, contract.MachineFailure(err)
		if err != nil {
			result.Failure = contract.CleanupFailure
		}
		return result, nil
	}
	target, err := checker.runtime.PrepareEdgeConnectionTarget(request.Action, request.Runtime, parameters)
	if err != nil {
		// Catalog validation never permits a replacement probe. Cleanup still
		// uses the captured identity and can prove the original helper absent.
		cleanupErr := executor.Cleanup(
			context.WithoutCancel(ctx),
			request.TaskId,
			request.ConnectorContainerId,
			parameters,
		)
		result.CleanupRequired, result.Failure = cleanupErr != nil, contract.MachineFailure(err)
		if cleanupErr != nil {
			result.Failure = contract.CleanupFailure
		}
		return result, nil
	}
	execution, err := executor.Execute(ctx, request.TaskId, request.ConnectorContainerId, target)
	result.CleanupRequired, result.Failure = execution.CleanupRequired, contract.MachineFailure(err)
	if execution.Result != nil {
		var output bytes.Buffer
		if writeErr := contract.WritePrivateResult(&output, *execution.Result, parameters); writeErr != nil {
			return nil, writeErr
		}
		result.PrivateResultJson = output.Bytes()
	}
	if result.CleanupRequired {
		result.Failure = contract.CleanupFailure
	}
	if err := contract.ValidateMachineResult(request, result); err != nil {
		return nil, err
	}
	return result, nil
}
