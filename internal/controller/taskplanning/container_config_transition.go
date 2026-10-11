package taskplanning

import (
	"bytes"
	"context"
	"crypto/sha256"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane/internal/common/envoyrouter"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (resolver *TaskPlanResolver) containerActivation(action *agentpb.ComponentApply) (
	registeredcatalog.ContainerConfigActivation, string, core.ComponentKind, error,
) {
	if resolver == nil || action == nil {
		return "", "", "", errs.New(errs.KindInternal, "container Component action resolver is missing")
	}
	for _, registration := range resolver.componentCatalog {
		managed := registration.ManagedConfiguration
		definitionDigest := registration.Definition.Digest()
		if managed != nil && string(managed.ActionID) == action.GetActionId() &&
			bytes.Equal(definitionDigest[:], action.GetDefinitionDigest()) &&
			bytes.Equal(registration.CatalogDigest[:], action.GetCatalogDigest()) {
			return managed.Activation, managed.SourcePath, registration.Kind, nil
		}
	}
	return "", "", "", errs.New(errs.KindStateConflict, "container Component action is not in the captured catalog")
}

// Bind before sealing. The same retained configuration references are used
// during reconstruction; no current head or rendered predecessor is consulted.
func (resolver *TaskPlanResolver) bindContainerConfigTransition(
	ctx context.Context, task etcd.TaskRecord, action *agentpb.ComponentApply,
) error {
	activation, destination, implementation, err := resolver.containerActivation(action)
	if err != nil {
		return err
	}
	if activation == registeredcatalog.ContainerConfigActivationCommand {
		if action.GetContainerConfigTransition() != nil {
			return errs.New(errs.KindStateConflict, "command activation contains HTTP discovery authority")
		}
		return nil
	}
	if activation != registeredcatalog.ContainerConfigActivationHTTPDiscovery ||
		resolver.configurationRecovery == nil ||
		ctx == nil ||
		task.Configuration == nil ||
		task.RenderGeneration <= 0 {
		return errs.New(errs.KindStateConflict, "HTTP discovery Task has no retained previous configuration")
	}
	var forward *taskmaterialization.Record
	for index := range task.Materializations {
		record := &task.Materializations[index]
		if record.MaterializationID != action.GetArtifactId() {
			continue
		}
		if forward != nil || record.Destination != destination || record.EnvironmentID != task.Owner.EnvironmentID ||
			record.Source.Kind != taskmaterialization.SourceComponentFile || record.Source.ComponentFile == nil ||
			record.Source.ComponentFile.ComponentID != action.GetComponentId() {
			return errs.New(errs.KindStateConflict, "HTTP discovery forward configuration ownership is ambiguous")
		}
		forward = record
	}
	if forward == nil {
		return errs.New(errs.KindStateConflict, "HTTP discovery forward configuration source is missing")
	}
	desired, err := resolver.loadComponentMaterialization(ctx, *forward, uint64(task.RenderGeneration))
	if err != nil {
		return err
	}
	if _, err := envoyrouter.Read(desired); err != nil {
		clear(desired)
		return err
	}
	if task.Params[taskjournal.TaskBlueprintComponentUnitParam] == action.GetComponentId() {
		predecessorKind := core.ComponentKind("")
		for _, source := range task.ManagedComponentTeardownSources {
			if source.ComponentID != action.GetComponentId() {
				continue
			}
			if predecessorKind != "" {
				return errs.New(errs.KindStateConflict, "Component configuration predecessor is ambiguous")
			}
			predecessorKind = source.ComponentKind
		}
		if predecessorKind != implementation {
			// Initial activation or an engine change has no serving discovery
			// predecessor for this recipe. A file retained from an older, retired
			// Envoy instance cannot become active-source authority by path alone.
			action.ContainerConfigTransition = &agentpb.ContainerConfigTransition{DesiredSource: desired}
			return nil
		}
	}
	prepared, err := resolver.configurationRecovery.Prepare(ctx, task)
	if err != nil {
		return err
	}
	for index, file := range prepared.Files {
		if prepared.Procedure.Files[index].ForwardStepId != forward.StepID {
			continue
		}
		original := file.Original
		if original.Destination != destination || original.EnvironmentID != task.Owner.EnvironmentID {
			return errs.New(errs.KindStateConflict, "HTTP discovery predecessor belongs to another configuration")
		}
		transition := &agentpb.ContainerConfigTransition{DesiredSource: desired}
		if original.Source.Kind == taskmaterialization.SourceRemoval {
			action.ContainerConfigTransition = transition
			return nil
		}
		if original.Source.Kind != taskmaterialization.SourceComponentFile || original.Source.ComponentFile == nil ||
			original.Source.ComponentFile.ComponentID != action.GetComponentId() {
			return errs.New(
				errs.KindStateConflict,
				"HTTP discovery predecessor is not the same Component's retained file",
			)
		}
		content, err := resolver.loadComponentMaterialization(ctx, original, uint64(task.RenderGeneration))
		if err != nil {
			return err
		}
		if _, err := envoyrouter.PrepareActivation(content, desired); err != nil {
			clear(content)
			return err
		}
		digest := sha256.Sum256(content)
		transition.PreviousSource = content
		transition.PreviousSourceSha256 = append([]byte(nil), digest[:]...)
		action.ContainerConfigTransition = transition
		return nil
	}
	return errs.New(
		errs.KindStateConflict,
		"HTTP discovery predecessor is not selected by this Task's file restoration",
	)
}

func (resolver *TaskPlanResolver) prepareRouteContainerConfiguration(
	ctx context.Context, task etcd.TaskRecord, revision int64,
) (etcd.TaskRecord, error) {
	if resolver == nil || resolver.releases == nil {
		return etcd.TaskRecord{}, errs.New(errs.KindInternal, "Route configuration source repository is missing")
	}
	return resolver.releases.PrepareTaskConfigurationAtRevision(ctx, task, revision)
}
