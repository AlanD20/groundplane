package taskplanning

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Direct Routes and Blueprint Components share file and runtime compensation.
// Route recovery selects only the router captured at the admission snapshot;
// other Services present in its historical artifact are never executable targets.
func (resolver *TaskPlanResolver) sealRouteRecoveryPlan(
	ctx context.Context, task etcd.TaskRecord, provider *environmentchanges.RouteProviderPin,
	input taskplan.BuildInput,
) (etcd.TaskRecord, *agentpb.ExecutionPlan, error) {
	if provider == nil || len(input.Artifacts) != 1 || len(input.Steps) != 3 {
		return etcd.TaskRecord{}, nil, errs.New(errs.KindInternal, "Route recovery has no captured forward plan")
	}
	priorArtifact, err := environmentchanges.DecodeRouteProviderRuntime(provider)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	var prior *agentpb.ManagedComponentService
	if priorArtifact != nil {
		if priorArtifact.GetArtifactId() == input.Artifacts[0].GetArtifactId() {
			return etcd.TaskRecord{}, nil, errs.New(
				errs.KindStateConflict,
				"Route candidate reuses its predecessor artifact",
			)
		}
		input.Artifacts = append(input.Artifacts, priorArtifact)
		prior = &agentpb.ManagedComponentService{
			ComponentId: provider.ComponentID, ServiceId: provider.ServiceID,
			SourceArtifactId: priorArtifact.GetArtifactId(),
		}
		// The compiled kind owns the read-only configuration confirmation recipe.
		for _, registration := range resolver.componentCatalog {
			digest := registration.Definition.Digest()
			if hex.EncodeToString(digest[:]) == provider.DefinitionDigest &&
				hex.EncodeToString(registration.CatalogDigest[:]) == provider.CatalogDigest {
				prior.ComponentKind = string(registration.Kind)
			}
		}
		if prior.ComponentKind == "" {
			return etcd.TaskRecord{}, nil, errs.New(errs.KindStateConflict, "Route captured router is not registered")
		}
	}
	steps, recovery, err := resolver.prepareComponentRecovery(ctx, task, provider.ComponentID,
		input.Artifacts[0], input.Artifacts, prior, input.Steps)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	input.Steps, input.ComponentRecoveryProcedure = steps, recovery
	plan, err := taskplan.Build(input)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	task.Steps = make([]taskjournal.TaskStepRecord, len(plan.GetSteps()))
	for index, step := range plan.GetSteps() {
		task.Steps[index] = taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: step.GetStepId()}
	}
	return task, plan, nil
}
