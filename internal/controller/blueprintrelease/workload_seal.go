package blueprintrelease

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"

	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// WorkloadPreparation retains the exact historical authority checked alongside
// candidate images. New-Service provisional identities are never retained.
type WorkloadPreparation struct {
	candidates   map[string]domain.WorkloadSeal
	predecessors map[string]predecessorSnapshot
	retained     *retainedRuntime
}

// Preflight does no durable staging. Its temporary new-Service IDs are not
// retained: seals are bound to the final candidate by name, image and count.
func (service *Service) Preflight(
	ctx context.Context,
	environmentID string,
	changes []etcd.EnvironmentBlueprintServiceChange,
	memberships NormalizedServiceMemberships,
	groups map[string]core.ReleaseGroupSpec,
) (WorkloadPreparation, error) {
	candidates, err := selectCandidates(
		projectionrecord.EnvironmentComposeProjection{},
		changes,
		groups,
		memberships,
	)
	if err != nil {
		return WorkloadPreparation{}, err
	}
	result := WorkloadPreparation{
		candidates:   make(map[string]domain.WorkloadSeal, len(candidates)),
		predecessors: make(map[string]predecessorSnapshot),
	}
	result.retained, err = service.captureRetainedRuntime(ctx, environmentID, changes, candidates)
	if err != nil {
		return WorkloadPreparation{}, err
	}
	if len(candidates) == 0 {
		return result, nil
	}
	selections := make([]workloadseal.Selection, len(candidates))
	var scope *etcd.ReleasePlanningScope
	for index, candidate := range candidates {
		desired := candidate.Record.Desired
		if desired.Replicas < 1 || uint64(desired.Replicas) > uint64(^uint32(0)) {
			return WorkloadPreparation{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint workload replica count is invalid",
			)
		}
		selections[index] = workloadseal.Selection{
			Requested: &workloadseal.Requested{Reference: desired.Image, Replicas: uint32(desired.Replicas)},
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
				return WorkloadPreparation{}, scopeErr
			}
			scope = &loaded
		}
		if scope.ReadRevision != candidate.Current.ReadRevision {
			return WorkloadPreparation{}, errs.New(
				errs.KindStateConflict,
				"Blueprint predecessor capture revisions disagree",
			)
		}
		planning, planningErr := service.ledger.LoadPlanningServices(ctx, *scope, []string{desired.ID})
		if planningErr != nil {
			return WorkloadPreparation{}, planningErr
		}
		predecessor, captureErr := service.capturePredecessor(ctx, *scope, planning[0])
		if captureErr != nil {
			return WorkloadPreparation{}, captureErr
		}
		result.predecessors[desired.Name] = predecessor
		if predecessor.serving != nil {
			selections = append(selections, workloadseal.Selection{Historical: &predecessor.serving.CandidateWorkload})
		}
	}
	agent, err := service.agents.GetSingleton(ctx)
	if err != nil {
		return WorkloadPreparation{}, err
	}
	seals, err := workloadseal.Resolve(ctx, service.images, agent.Record.ID, selections)
	if err != nil {
		return WorkloadPreparation{}, err
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
