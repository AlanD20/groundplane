// Package rollingpredecessor captures physical members before a scaled recreate
// workload is replaced by rolling. It does not mutate containers or publish work.
package rollingpredecessor

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Observer interface {
	ObserveServices(
		context.Context,
		string,
		[]*agentpb.ServiceObservationTarget,
	) (*agentpb.ServiceObservationResult, error)
}

type AgentReader interface {
	GetSingleton(context.Context) (keyvalue.Versioned[localagents.LocalAgentRecord], error)
}

// Capture shares preparation between ordinary Deploy/rollback and Blueprint.
// The publication's existing MVCC guards still fence the captured predecessor;
// execution rechecks each immutable id because host state can change meanwhile.
func Capture(
	ctx context.Context,
	agents AgentReader,
	channel Observer,
	input *releaserender.ReleaseRenderInput,
	timeout uint32,
) error {
	if input == nil {
		return errs.New(errs.KindInternal, "rolling predecessor render input is missing")
	}
	if input.Strategy != domain.StrategyRolling || input.PriorStrategy != domain.StrategyRecreate ||
		input.PriorWorkload == nil || input.PriorWorkload.ReplicaCount <= 1 {
		return releaserender.CaptureRollingPlan(input, timeout, nil)
	}
	if input.PriorRuntime == nil || input.PriorWorkload.ReplicaCount > rollingrelease.MaximumReplicas {
		return errs.New(
			errs.KindValidationFailed,
			"rolling requires an acknowledged predecessor with at most 16 replicas",
		)
	}
	target, count, err := serviceruntimerecord.NativeWorkloadObservation(input.EnvironmentID, input.ServiceID,
		input.PriorRuntime.CurrentArtifact, input.PriorRuntime.RetainedPriorArtifact)
	if err != nil {
		return err
	}
	if target.GetRuntimeRole() != "singleton" || len(target.GetWorkloads()) != 1 ||
		count != input.PriorWorkload.ReplicaCount {
		return errs.New(errs.KindStateConflict, "scaled predecessor observation differs from its captured runtime")
	}
	agent, err := agents.GetSingleton(ctx)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, serviceobservation.Timeout)
	defer cancel()
	result, err := channel.ObserveServices(ctx, agent.Record.ID, []*agentpb.ServiceObservationTarget{target})
	if err != nil {
		return err
	}
	if result == nil {
		return errs.New(errs.KindStateConflict, "scaled predecessor container observation is unavailable")
	}
	if err := serviceobservation.ValidateResult(&agentpb.ObserveServices{RequestId: result.GetRequestId(),
		Targets: []*agentpb.ServiceObservationTarget{target}}, result); err != nil {
		return err
	}
	row := result.GetObservations()[0]
	if row.GetReplicas() == nil || len(row.GetContainers()) != int(count) ||
		target.GetProxyComposeName() != "" &&
			row.GetProxyState() != agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MATCHING {
		return errs.New(
			errs.KindStateConflict,
			"scaled predecessor containers or serving proxy are not ready for rolling replacement",
		)
	}
	containers := slices.Clone(row.GetContainers())
	slices.SortFunc(
		containers,
		func(a, b *agentpb.ServiceContainerObservation) int { return int(a.GetReplica()) - int(b.GetReplica()) },
	)
	prior := make([]rollingrelease.Replica, len(containers))
	for index, container := range containers {
		if container.GetReplica() != uint32(index+1) || container.GetImage() != input.PriorWorkload.LocalImageID ||
			container.GetState() != "running" || container.GetHealth() != "healthy" && container.GetHealth() != "none" {
			return errs.New(errs.KindStateConflict, "scaled predecessor container identity, image or readiness changed")
		}
		prior[index] = rollingrelease.Replica{ReleaseID: target.GetReleaseId(), ArtifactID: input.PriorArtifactID,
			ComposeName: target.GetWorkloads()[0].GetComposeName(), Ordinal: uint32(index + 1),
			ContainerID: container.GetId(), ContainerName: container.GetName(), ComposeReplica: container.GetReplica()}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return releaserender.CaptureRollingPlan(input, timeout, prior)
}
