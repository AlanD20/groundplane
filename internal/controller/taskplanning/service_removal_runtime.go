package taskplanning

import (
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Preserve each physical member's acknowledged bytes and ownership. The stable
// proxy and the two workload slots can belong to different historical plans.
func serviceRuntimeArtifacts(
	task etcd.TaskRecord,
	serviceID string,
	runtime *serviceruntimerecord.Record,
) ([]*agentpb.ComposeArtifact, error) {
	if runtime == nil {
		return nil, errs.New(errs.KindInternal, "Service acknowledged runtime is missing")
	}
	if err := serviceruntimerecord.Validate(*runtime); err != nil {
		return nil, err
	}
	var artifacts []*agentpb.ComposeArtifact
	seen := make(map[string]bool)
	for _, raw := range [][]byte{runtime.Runtime.CurrentArtifact, runtime.Runtime.RetainedPriorArtifact} {
		if len(raw) == 0 {
			continue
		}
		source := new(agentpb.ComposeArtifact)
		if proto.Unmarshal(raw, source) != nil {
			return nil, errs.New(errs.KindInternal, "Service removal runtime artifact is corrupt")
		}
		for _, member := range source.Services {
			if seen[member.ComposeName] || member.ServiceId != serviceID {
				return nil, errs.New(errs.KindStateConflict, "Service removal physical member is duplicated or foreign")
			}
			seen[member.ComposeName] = true
			artifact, err := pruneServiceLifecycleArtifact(
				proto.CloneOf(source),
				map[string]bool{member.ComposeName: true},
			)
			if err != nil {
				return nil, err
			}
			artifact.ArtifactId = ids.DeriveAt(ids.KindConfig, task.CreatedAt, task.ID, member.ComposeName)
			artifacts = append(artifacts, artifact)
		}
	}
	return artifacts, nil
}

func (resolver *TaskPlanResolver) buildAcknowledgedServiceRemovalPlan(
	task etcd.TaskRecord,
	intent environmentchanges.ServiceRemovalIntent,
) (*agentpb.ExecutionPlan, error) {
	artifacts, err := serviceRuntimeArtifacts(task, intent.ServiceID, intent.AcknowledgedRuntime)
	if err != nil {
		return nil, err
	}
	if len(artifacts) != len(task.Steps) {
		return nil, errs.New(errs.KindInternal, "Service removal runtime procedure changed")
	}
	sources := make([]*agentpb.ServiceLifecycleSource, len(artifacts))
	steps := make([]*agentpb.ExecutionStep, len(artifacts))
	for index, artifact := range artifacts {
		source, sourceErr := serviceRuntimeSource(artifact, task.Steps[index].ID)
		if sourceErr != nil {
			return nil, sourceErr
		}
		sources[index] = source
		steps[index] = &agentpb.ExecutionStep{
			StepId: source.StepId, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
				ArtifactId: artifact.ArtifactId, ServiceIds: []string{intent.ServiceID},
			}},
		}
		if index > 0 {
			steps[index].PrerequisiteStepId = steps[index-1].StepId
		}
	}
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
		Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetID: intent.ServiceID,
		Artifacts: artifacts, Steps: steps, ServiceLifecycleProcedure: &agentpb.ServiceLifecycleProcedure{Sources: sources},
	})
}

func serviceRuntimeSource(artifact *agentpb.ComposeArtifact, stepID string) (*agentpb.ServiceLifecycleSource, error) {
	member := artifact.Services[0]
	source := &agentpb.ServiceLifecycleSource{
		ArtifactId: artifact.ArtifactId, ServiceId: member.ServiceId,
		ComposeNames: []string{member.ComposeName}, StepId: stepID,
	}
	for _, label := range member.ExpectedLabels {
		switch label.Key {
		case "com.groundplane.plan-id":
			source.SourcePlanId = label.Value
		case "com.groundplane.render-generation":
			generation, err := strconv.ParseUint(label.Value, 10, 64)
			if err != nil {
				return nil, errs.New(errs.KindInternal, "Service runtime generation is invalid")
			}
			source.SourceRenderGeneration = generation
		}
	}
	return source, nil
}
