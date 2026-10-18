package composehelper

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ExecuteComponentWork(ctx context.Context, taskRunner runner.Runner, request *agentpb.ComponentWorkHelperRequest, catalog ComponentActionCatalog) (*agentpb.ComponentWorkHelperResponse, error) {
	input, err := componentexecution.OpenHelperRequest(request)
	if err != nil {
		return nil, err
	}
	if ctx == nil || taskRunner == nil || catalog == nil {
		return nil, errs.New(errs.KindInternal, "Component helper is not configured")
	}
	bounded, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	if input.InspectOnly {
		cancel()
		bounded, cancel = context.WithTimeout(ctx, 15*time.Second)
	}
	defer cancel()
	if input.Recovery != nil {
		return executeComponentRecovery(bounded, taskRunner, input, catalog)
	}
	observation, err := observeComponentWork(bounded, taskRunner, input, catalog)
	if err != nil {
		return nil, err
	}
	if !input.InspectOnly && !observation.Satisfies(input.Progress.Stage, input.Procedure.Remove) {
		switch input.Progress.Stage {
		case componentexecution.ReconcileRuntime:
			err = reconcileComponentRuntime(bounded, taskRunner, input)
			if err == nil {
				err = waitComponentRuntime(bounded, taskRunner, input, false)
			}
		case componentexecution.RemoveRuntime:
			err = removeComponentRuntime(bounded, taskRunner, input)
		case componentexecution.ActivateConfiguration:
			err = activateComponentWork(bounded, taskRunner, input.Procedure.Candidate, input.Procedure.ConfigurationAction, catalog)
		case componentexecution.VerifyRuntime:
			err = waitComponentRuntime(bounded, taskRunner, input, false)
		default:
			return nil, errs.New(errs.KindValidationFailed, "Component stage is not a Docker runtime operation")
		}
		if err != nil {
			return nil, err
		}
		observation, err = observeComponentWork(bounded, taskRunner, input, catalog)
		if err != nil {
			return nil, err
		}
	}
	return componentexecution.HelperObservation(input, observation)
}

func observeComponentWork(ctx context.Context, taskRunner runner.Runner, input componentexecution.HelperInput, catalog ComponentActionCatalog) (componentexecution.Observation, error) {
	observed := componentexecution.Observation{ComponentID: input.Procedure.ComponentId, Input: input.Attempt.Input, Stage: input.Progress.Stage}
	inventory, err := inspectComponentWork(ctx, taskRunner, input)
	if err != nil {
		return observed, err
	}
	absent := len(inventory.Candidate)+len(inventory.Prior) == 0
	ready := componentWorkReady(inventory.Candidate) && len(inventory.Prior) == 0
	switch input.Progress.Stage {
	case componentexecution.ReconcileRuntime:
		observed.RuntimeReconciled = ready
	case componentexecution.RemoveRuntime:
		observed.RuntimeAbsent = absent
	case componentexecution.ActivateConfiguration:
		if ready {
			observed.ConfigurationActivated, err = observeComponentActivation(ctx, taskRunner, input.Procedure.Candidate, input.Procedure.ConfigurationAction, catalog)
		}
	case componentexecution.VerifyRuntime:
		if input.Procedure.Remove {
			observed.RuntimeAbsent = absent
		} else if ready {
			observed.RuntimeReady, err = observeComponentActivation(ctx, taskRunner, input.Procedure.Candidate, input.Procedure.ConfigurationAction, catalog)
		}
	default:
		return observed, errs.New(errs.KindValidationFailed, "Component observation stage is not a Docker runtime operation")
	}
	observed.At = time.Now().UTC()
	return observed, err
}
