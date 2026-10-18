package blueprintrelease

import (
	"context"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// LoadWorkAssignment delivers immutable operation input, not permission to run.
// It deliberately does not use current desired state: an interrupted attempt
// must still be able to receive its exact captured input for reconciliation.
func (service *Service) LoadWorkAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.ReleaseWorkAssignment, error) {
	if !view.CurrentExecutor {
		return nil, errs.New(errs.KindStateConflict, "Release delivery requires the current assigned executor")
	}
	return service.loadCapturedAssignment(ctx, view)
}

// Reading retained input grants no permission to send it or execute effects.
// Reconciliation uses the original identity even after its process is replaced.
func (service *Service) loadCapturedAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.ReleaseWorkAssignment, error) {
	if service == nil || service.ledger == nil || !view.ActiveAttempt || view.ExecutorRevision <= 0 ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return nil, errs.New(errs.KindStateConflict, "Release input requires its retained assignment")
	}
	sources, err := service.ledger.LoadBlueprintReleaseWorkInput(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	if len(sources.Predecessors) != 1 || sources.Predecessors[0].ServiceID != view.Work.Item.Item.TargetID {
		return nil, errs.New(errs.KindStateConflict, "Release delivery has no exact Service predecessor")
	}
	capture, render := sources.Rollout.Record, sources.Rollout.Record.Render
	candidate, err := releaserender.OpenBlueprintCandidateArtifact(capture)
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(view.Attempt.Attempt)
	if err != nil {
		return nil, err
	}
	configuration, err := releaseConfigurationWire(sources.Input.Configuration.Current)
	if err != nil {
		return nil, err
	}
	input := &agentpb.ReleaseWorkInput{
		OperationId: capture.OperationID, EnvironmentId: capture.Scope.EnvironmentID,
		ServiceId: render.ServiceID, ReleaseId: render.ReleaseID, Candidate: candidate,
		RenderGeneration: render.Projection.RenderGeneration,
		Configuration:    configuration, ConfigurationDestinations: slices.Clone(sources.Input.Configuration.Destinations),
		Predecessor: &agentpb.ReleaseWorkPredecessor{Absent: true},
	}
	if prior := sources.Input.Configuration.Prior; prior != nil {
		input.PriorConfiguration, err = releaseConfigurationWire(*prior)
		if err != nil {
			return nil, err
		}
	}
	predecessor := sources.Predecessors[0]
	if predecessor.Serving != nil {
		runtime, err := executionplan.OpenNativeServingRuntime(input.EnvironmentId, input.ServiceId,
			predecessor.CurrentArtifact, predecessor.RetainedPriorArtifact)
		if err != nil {
			return nil, err
		}
		if runtime.ReleaseID != predecessor.Serving.ServingReleaseID || runtime.Target != string(predecessor.Serving.Target) {
			return nil, errs.New(errs.KindStateConflict, "Release predecessor bytes differ from the captured serving identity")
		}
		input.Predecessor = &agentpb.ReleaseWorkPredecessor{ReleaseId: runtime.ReleaseID, Target: runtime.Target,
			CurrentArtifact: slices.Clone(predecessor.CurrentArtifact), RetainedArtifact: slices.Clone(predecessor.RetainedPriorArtifact)}
	} else if len(predecessor.CurrentArtifact) != 0 || len(predecessor.RetainedPriorArtifact) != 0 {
		return nil, errs.New(errs.KindStateConflict, "Release predecessor absence contains a runtime")
	}
	switch render.Strategy {
	case domain.StrategyRecreate:
		input.Strategy = agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE
	case domain.StrategyBlueGreen:
		input.Strategy = agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN
	case domain.StrategyRolling:
		input.Strategy = agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_ROLLING
		if render.Rolling == nil || render.Rolling.Validate() != nil {
			return nil, errs.New(errs.KindStateConflict, "Release rolling input is missing or invalid")
		}
		input.Rolling = &agentpb.ReleaseWorkRolling{ReplicaTimeoutSeconds: render.Rolling.ReplicaTimeoutSeconds,
			Prior: releaseReplicaWire(render.Rolling.Prior), Candidate: releaseReplicaWire(render.Rolling.Candidate),
			Retained: releaseReplicaWire(render.Rolling.Retained)}
	default:
		return nil, errs.New(errs.KindStateConflict, "Release strategy is unsupported")
	}
	for _, hook := range render.Hooks {
		if hook.Input.Validate() != nil || hook.ServiceID != input.ServiceId {
			return nil, errs.New(errs.KindStateConflict, "Release hook does not belong to the captured Service")
		}
		digest, err := hex.DecodeString(hook.Input.SHA256)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		input.Hooks = append(input.Hooks, &agentpb.ReleaseWorkHook{
			Input:       &agentpb.WorkInputReference{InputId: hook.Input.ID, Sha256: digest},
			ExecutionId: hook.ScriptExecutionID, ScriptId: hook.ScriptID, Phase: string(hook.When), Order: uint32(hook.Order),
		})
	}
	if len(render.ProxyPorts) != 0 {
		if render.Rolling != nil {
			changes, err := render.Rolling.Changes()
			if err != nil {
				return nil, err
			}
			if render.ProxyGeneration < uint64(len(changes)) {
				return nil, errs.New(errs.KindStateConflict, "Release proxy generation cannot cover its rolling changes")
			}
			first := render.ProxyGeneration - uint64(len(changes)) + 1
			for index := range changes {
				configuration, err := domain.RenderRollingProxyConfig(render.ProxyImplementation, *render.Rolling, uint32(index+1), first+uint64(index), render.ProxyPorts)
				if err != nil {
					return nil, err
				}
				input.TrafficConfigurations = append(input.TrafficConfigurations, configuration.JSON)
			}
		} else {
			configuration, err := domain.RenderProxyConfig(render.ProxyImplementation, render.ServiceName, render.ReleaseID, render.CandidateTarget, render.ProxyGeneration, render.ProxyPorts)
			if err != nil {
				return nil, err
			}
			input.TrafficConfigurations = [][]byte{configuration.JSON}
		}
	}
	if _, err := executionplan.OpenReleaseWorkArtifacts(view.Attempt.Attempt, input); err != nil {
		return nil, err
	}
	return &agentpb.ReleaseWorkAssignment{Attempt: attempt, Release: input}, nil
}

func releaseConfigurationWire(reference runtimeconfiguration.Reference) (*agentpb.WorkConfigurationReference, error) {
	if err := runtimeconfiguration.ValidateReference(reference); err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return &agentpb.WorkConfigurationReference{ConfigurationId: reference.ID, EnvironmentId: reference.EnvironmentID,
		Generation: reference.Generation, Sha256: digest}, nil
}

func releaseReplicaWire(replicas []rollingrelease.Replica) []*agentpb.RollingReplica {
	result := make([]*agentpb.RollingReplica, 0, len(replicas))
	for _, replica := range replicas {
		result = append(result, &agentpb.RollingReplica{ReleaseId: replica.ReleaseID, ArtifactId: replica.ArtifactID,
			ComposeName: replica.ComposeName, Ordinal: replica.Ordinal, ContainerId: replica.ContainerID,
			ContainerName: replica.ContainerName, ComposeReplica: replica.ComposeReplica})
	}
	return result
}
