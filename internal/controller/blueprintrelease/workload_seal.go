package blueprintrelease

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	releasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"

	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// workloadPreparation captures candidate images and their exact predecessor
// authority for the selected Service unit.
type workloadPreparation struct {
	candidates   map[string]domain.WorkloadSeal
	predecessors map[string]predecessorSnapshot
	proxyImages  map[string]domain.ProxyImage
}

func (service *Service) prepareSelectedWorkloads(
	ctx context.Context,
	projection projectionrecord.EnvironmentComposeProjection,
	candidates []blueprints.EnvironmentBlueprintServiceChange,
	replacement *componentplanning.ProxyReplacementImage,
) (workloadPreparation, error) {
	result := workloadPreparation{
		candidates:   make(map[string]domain.WorkloadSeal, len(candidates)),
		predecessors: make(map[string]predecessorSnapshot),
		proxyImages:  make(map[string]domain.ProxyImage),
	}
	if replacement != nil && (len(candidates) != 1 || candidates[0].Record.Desired.ID != replacement.ServiceID) {
		return workloadPreparation{}, errs.New(
			errs.KindStateConflict,
			"proxy replacement image belongs to another Service unit",
		)
	}
	var err error
	environmentID := projection.EnvironmentID
	if len(candidates) != 1 {
		return workloadPreparation{}, errs.New(
			errs.KindInternal,
			"Blueprint workload preparation requires one Service unit",
		)
	}
	selections := make([]workloadseal.Selection, len(candidates))
	var scope *releasequeries.ReleasePlanningScope
	var proxyImplementation serviceproxy.Implementation
	for index, candidate := range candidates {
		desired := candidate.Record.Desired
		if desired.Replicas < 1 || uint64(desired.Replicas) > uint64(^uint32(0)) {
			return workloadPreparation{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint workload replica count is invalid",
			)
		}
		strategy, strategyErr := domain.DeploymentStrategy("", string(desired.Strategy))
		if strategyErr != nil {
			return workloadPreparation{}, strategyErr
		}
		if err := domain.ValidateDeploymentReadiness(desired.Name, strategy, uint32(desired.Replicas), desired.Healthcheck.Enabled()); err != nil {
			return workloadPreparation{}, err
		}
		exposures := desired.ProxyExposures()
		proxySelection := domain.ProxySelection{}
		if strategy == domain.StrategyBlueGreen {
			if len(exposures) == 0 {
				return workloadPreparation{}, errs.New(
					errs.KindValidationFailed,
					"blue-green requires an exposed container TCP port; configure expose or select recreate for a portless Service",
				)
			}
		}
		if len(exposures) != 0 {
			if proxyImplementation == "" {
				proxyImplementation, err = projectionrecord.ServiceProxyImplementation(projection)
				if err != nil {
					return workloadPreparation{}, err
				}
			}
			ports, err := domain.ProxyPorts(exposures)
			if err != nil {
				return workloadPreparation{}, err
			}
			if err := domain.ValidateProxyForwarding(proxyImplementation, ports); err != nil {
				return workloadPreparation{}, err
			}
			proxySelection = domain.ProxySelection{Implementation: proxyImplementation, Ports: ports}
		}
		selections[index] = workloadseal.Selection{
			Requested: &workloadseal.Requested{Reference: desired.Image, Replicas: uint32(desired.Replicas)},
		}
		if replacement != nil {
			if replacement.Workload.RequestedReference != desired.Image ||
				replacement.Workload.ReplicaCount != uint32(
					desired.Replicas,
				) || domain.ValidateWorkloadSeal(replacement.Workload) != nil ||
				replacement.Proxy.Validate() != nil || proxySelection.Implementation == "" {
				return workloadPreparation{}, errs.New(
					errs.KindStateConflict,
					"proxy replacement image differs from its sealed Service input",
				)
			}
			selections[index] = workloadseal.Selection{Historical: &replacement.Workload}
			result.proxyImages[desired.Name] = replacement.Proxy
		}
		if candidate.Current == nil {
			continue
		}
		if scope == nil {
			loaded, scopeErr := service.ledger.LoadPlanningScopeAtRevision(
				ctx,
				environmentID,
				candidate.Current.ReadRevision,
			)
			if scopeErr != nil {
				return workloadPreparation{}, scopeErr
			}
			scope = &loaded
		}
		if scope.ReadRevision != candidate.Current.ReadRevision {
			return workloadPreparation{}, errs.New(
				errs.KindStateConflict,
				"Blueprint predecessor capture revisions disagree",
			)
		}
		planning, planningErr := service.ledger.LoadPlanningServices(ctx, *scope, []string{desired.ID})
		if planningErr != nil {
			return workloadPreparation{}, planningErr
		}
		predecessor, captureErr := service.capturePredecessor(ctx, *scope, planning[0])
		if captureErr != nil {
			return workloadPreparation{}, captureErr
		}
		result.predecessors[desired.Name] = predecessor
		if predecessor.serving != nil {
			previous := domain.ProxySelection{
				Implementation: predecessor.historical.Record.ProxyImplementation,
				Ports:          predecessor.historical.Record.ProxyPorts,
			}
			if err := domain.ValidateProxyTransition(desired.Name, strategy, proxySelection, &previous); err != nil {
				return workloadPreparation{}, err
			}
			selections = append(selections, workloadseal.Selection{Historical: &predecessor.serving.CandidateWorkload})
		}
	}
	agent, err := service.agents.GetSingleton(ctx)
	if err != nil {
		return workloadPreparation{}, err
	}
	seals, err := workloadseal.Resolve(ctx, service.images, agent.Record.ID, selections)
	if err != nil {
		return workloadPreparation{}, err
	}
	for index, candidate := range candidates {
		result.candidates[candidate.Record.Desired.Name] = seals[index]
	}
	return result, nil
}

func sealedCandidate(workloads map[string]domain.WorkloadSeal, service core.Service) (domain.WorkloadSeal, error) {
	seal, exists := workloads[service.Name]
	if !exists || domain.ValidateWorkloadSeal(seal) != nil ||
		seal.RequestedReference != service.Image || int64(seal.ReplicaCount) != int64(service.Replicas) {
		return domain.WorkloadSeal{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint candidate has no matching preflight workload seal",
		)
	}
	return seal, nil
}
