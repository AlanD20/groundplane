package composehelper

import (
	"context"
	"encoding/hex"
	"time"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/dnsresolverobserver"
	"github.com/AlanD20/groundplane/internal/infra/docker/hostresolutionhelper"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ResolverCatalog interface {
	ResolveManagedResolverAction(*agentpb.ComponentApply) (registeredcatalog.ManagedConfigActionRecipe, error)
	ResolveResolverObservation(*agentpb.ComponentApply) (registeredcatalog.DNSResolverObservationRecipe, error)
}

func ExecuteResolverWork(ctx context.Context, taskRunner runner.Runner, request *agentpb.ResolverWorkHelperRequest,
	catalog ResolverCatalog,
) (*agentpb.ResolverWorkHelperResponse, error) {
	input, err := resolverexecution.OpenHelperRequest(request)
	if err != nil {
		return nil, err
	}
	if ctx == nil || taskRunner == nil || catalog == nil {
		return nil, errs.New(errs.KindInternal, "DNS resolver helper is not configured")
	}
	bounded, cancel := context.WithDeadline(ctx, input.Assignment.Attempt.Deadline)
	if input.InspectOnly {
		cancel()
		bounded, cancel = context.WithTimeout(ctx, 60*time.Second)
	}
	defer cancel()
	if input.Assignment.Recovery != nil {
		return restoreResolverWork(bounded, taskRunner, input, catalog)
	}
	observation, proof, err := observeResolverWork(bounded, taskRunner, input, catalog)
	if err != nil {
		return nil, err
	}
	if !input.InspectOnly && !observation.Satisfies(input.Progress.Stage, input.Layout) {
		switch input.Progress.Stage {
		case resolverexecution.WriteConfiguration:
			err = writeResolverConfiguration(bounded, input, catalog)
		case resolverexecution.ReconcileRuntime:
			err = reconcileResolverRuntime(bounded, taskRunner, input, false)
		case resolverexecution.ApplyHostResolver:
			_, err = observeResolverDNS(bounded, *input.Assignment.Input.Candidate, catalog)
			if err == nil {
				err = hostresolutionhelper.ApplyCaptured(bounded, input.Assignment.Input.BaselineSHA256)
			}
			if err == nil {
				err = finalizeResolverConfiguration(bounded, input, catalog)
			}
		case resolverexecution.RestoreHostResolver:
			err = hostresolutionhelper.RestoreCaptured(bounded, input.Assignment.Input.BaselineSHA256)
		case resolverexecution.RemoveRuntime:
			err = removeResolverRuntime(bounded, taskRunner, input)
		case resolverexecution.VerifyAbsent, resolverexecution.VerifyDNS:
			// Observation-only stages never acquire mutation semantics.
		default:
			err = errs.New(errs.KindValidationFailed, "DNS resolver helper stage is unsupported")
		}
		if err != nil {
			return nil, err
		}
		observation, proof, err = observeResolverWork(bounded, taskRunner, input, catalog)
		if err != nil {
			return nil, err
		}
	}
	return resolverexecution.HelperObservation(input, observation, proof)
}

func observeResolverWork(ctx context.Context, taskRunner runner.Runner, input resolverexecution.HelperInput,
	catalog ResolverCatalog,
) (resolverexecution.Observation, *agentpb.DNSResolverObservationEvidence, error) {
	observation := resolverexecution.Observation{
		Input:       input.Layout.Input,
		ComponentID: input.Layout.ComponentID,
		Stage:       input.Progress.Stage,
	}
	configuration := input.Assignment.Input
	inventory, err := inspectResolverRuntime(ctx, taskRunner, input)
	if err != nil {
		return observation, nil, err
	}
	switch input.Progress.Stage {
	case resolverexecution.WriteConfiguration:
		request, _, err := resolverFileRequest(
			input,
			catalog,
			agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_PUBLISH,
		)
		if err != nil {
			return observation, nil, err
		}
		state, err := managedconfighelper.InspectNative(ctx, request.RelativePath)
		if err != nil {
			return observation, nil, err
		}
		if state.Present && state.Owner == request.TransactionId &&
			hex.EncodeToString(state.SHA256[:]) == input.Layout.ConfigurationSHA256 {
			observation.ConfigurationSHA256 = input.Layout.ConfigurationSHA256
		}
	case resolverexecution.ReconcileRuntime:
		observation.RuntimePresent = resolverRuntimeReady(inventory, configuration.Candidate.Runtime)
	case resolverexecution.VerifyDNS:
		if !resolverRuntimeReady(inventory, configuration.Candidate.Runtime) {
			break
		}
		proof, err := observeResolverDNS(ctx, *configuration.Candidate, catalog)
		if err != nil {
			return observation, nil, err
		}
		observed, err := resolverexecution.ServingObservation(input.Assignment.Attempt, configuration, proof)
		return observed, proof, err
	case resolverexecution.ApplyHostResolver, resolverexecution.RestoreHostResolver:
		state, err := hostresolutionhelper.InspectCaptured(ctx, configuration.BaselineSHA256)
		if err != nil {
			return observation, nil, err
		}
		if input.Progress.Stage == resolverexecution.ApplyHostResolver && state.Managed {
			// Even an already-local host resolver needs its actual DNS proof
			// and file finalization before this new generation is complete.
			if _, err := observeResolverDNS(ctx, *configuration.Candidate, catalog); err != nil {
				return observation, nil, err
			}
			committed, err := resolverConfigurationCommitted(ctx, input, catalog)
			if err != nil {
				return observation, nil, err
			}
			if committed {
				observation.HostResolver = "local_resolver"
			}
		} else if input.Progress.Stage == resolverexecution.RestoreHostResolver && state.Baseline {
			observation.HostResolver = "baseline"
		}
	case resolverexecution.RemoveRuntime, resolverexecution.VerifyAbsent:
		observation.RuntimeAbsent = len(inventory) == 0
	default:
		return observation, nil, errs.New(errs.KindValidationFailed, "DNS resolver observation stage is unsupported")
	}
	observation.At = time.Now().UTC()
	return observation, nil, nil
}

func observeResolverDNS(
	ctx context.Context,
	configuration resolverexecution.Configuration,
	catalog ResolverCatalog,
) (*agentpb.DNSResolverObservationEvidence, error) {
	recipe, err := catalog.ResolveResolverObservation(configuration.Observe)
	if err != nil {
		return nil, err
	}
	request, err := dnsresolverobserver.CapturedRequest(configuration.Runtime, configuration.Observe, recipe)
	if err != nil {
		return nil, err
	}
	observer, err := dnsresolverobserver.New()
	if err != nil {
		return nil, err
	}
	defer observer.Close()
	proof, err := observer.Observe(ctx, request)
	if err != nil {
		return nil, err
	}
	return proof, resolverexecution.ValidateServingProof(configuration, proof)
}

func restoreResolverWork(ctx context.Context, taskRunner runner.Runner, input resolverexecution.HelperInput,
	catalog ResolverCatalog,
) (*agentpb.ResolverWorkHelperResponse, error) {
	configuration := input.Assignment.Input
	if !configuration.PreviousEnabled {
		if err := hostresolutionhelper.RestoreCaptured(ctx, configuration.BaselineSHA256); err != nil {
			return nil, err
		}
	}
	if err := restoreResolverConfiguration(ctx, input, catalog); err != nil {
		return nil, err
	}
	if err := reconcileResolverRuntime(ctx, taskRunner, input, true); err != nil {
		return nil, err
	}
	var proof *agentpb.DNSResolverObservationEvidence
	if configuration.PreviousEnabled {
		var err error
		proof, err = observeResolverDNS(ctx, *configuration.Previous, catalog)
		if err != nil {
			return nil, err
		}
		if err := hostresolutionhelper.ApplyCaptured(ctx, configuration.BaselineSHA256); err != nil {
			return nil, err
		}
	}
	state, err := hostresolutionhelper.InspectCaptured(ctx, configuration.BaselineSHA256)
	if err != nil {
		return nil, err
	}
	if configuration.PreviousEnabled && !state.Managed || !configuration.PreviousEnabled && !state.Baseline {
		return nil, errs.New(
			errs.KindStateConflict,
			"DNS resolver restoration did not restore its captured host DNS state",
		)
	}
	return resolverexecution.HelperRestoration(
		input,
		resolverexecution.RestorationObservation{Input: input.Layout.Input,
			ComponentID: input.Layout.ComponentID, ForwardID: input.Assignment.Recovery.Forward.ID,
			ConfigurationRestored: true, RuntimeRestored: true, HostResolverRestored: true, ServingProof: proof, ObservedAt: time.Now().UTC()},
	)
}
