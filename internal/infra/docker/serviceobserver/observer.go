// Package serviceobserver reads only the Controller-selected serving workloads.
package serviceobserver

import (
	"context"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// Engine exposes no mutation, log, exec or storage operation. The composition
// root owns its connection lifetime, including Close.
type Engine interface {
	ContainerList(context.Context, client.ContainerListOptions) (client.ContainerListResult, error)
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
}

type Observer struct{ engine Engine }

func NewWithEngine(engine Engine) (*Observer, error) {
	if engine == nil {
		return nil, errs.New(errs.KindValidationFailed, "service observation engine is required")
	}
	return &Observer{engine: engine}, nil
}

func (observer *Observer) Observe(
	ctx context.Context,
	request *agentpb.ObserveServices,
) (*agentpb.ServiceObservationResult, error) {
	if ctx == nil || observer == nil || observer.engine == nil {
		return nil, errs.New(errs.KindInternal, "service observer is not configured")
	}
	if err := serviceobservation.ValidateRequest(request); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, serviceobservation.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	listed, err := observer.engine.ContainerList(ctx, client.ContainerListOptions{
		All: true,
		// One extra row detects truncation; it is never inspected or counted.
		Limit:   serviceobservation.MaximumContainers + 1,
		Filters: client.Filters{}.Add("label", "com.groundplane.environment-id="+request.Targets[0].EnvironmentId),
	})
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if len(listed.Items) > serviceobservation.MaximumContainers {
		return nil, errs.New(errs.KindResourceInUse, "service observation container limit exceeded")
	}
	result := &agentpb.ServiceObservationResult{RequestId: request.RequestId}
	for _, target := range request.Targets {
		row, observeErr := observer.observeTarget(ctx, target, listed.Items)
		if observeErr != nil {
			return nil, observeErr
		}
		result.Observations = append(result.Observations, row)
	}
	return result, nil
}

func (observer *Observer) observeTarget(
	ctx context.Context,
	target *agentpb.ServiceObservationTarget,
	candidates []container.Summary,
) (*agentpb.ServiceObservationRow, error) {
	row := &agentpb.ServiceObservationRow{
		ServiceId: target.ServiceId, ReleaseId: target.ReleaseId,
		Outcome: &agentpb.ServiceObservationRow_Unavailable{Unavailable: true},
	}
	counts := &agentpb.ServiceReplicaCounts{}
	seenIDs := make(map[string]bool)
	seenReplicas := make(map[uint64]bool)
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !selected(candidate.Labels, target) {
			continue
		}
		replica, valid := replicaIdentity(candidate.Labels, target)
		if !valid || candidate.ID == "" || seenIDs[candidate.ID] || seenReplicas[replica] {
			return row, nil
		}
		seenIDs[candidate.ID], seenReplicas[replica] = true, true
		inspected, err := observer.engine.ContainerInspect(ctx, candidate.ID, client.ContainerInspectOptions{})
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		if err != nil || inspected.Container.ID != candidate.ID || inspected.Container.Config == nil {
			return row, nil
		}
		actualReplica, valid := replicaIdentity(inspected.Container.Config.Labels, target)
		if !valid || actualReplica != replica || !countState(counts, inspected.Container.State) {
			return row, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	row.Outcome = &agentpb.ServiceObservationRow_Replicas{Replicas: counts}
	return row, nil
}

// Retained releases, proxies, one-off containers and Components are not serving
// candidates. A selected candidate with corrupt ownership is unavailable, not
// silently omitted from an otherwise healthy count.
func selected(labels map[string]string, target *agentpb.ServiceObservationTarget) bool {
	return labels["com.groundplane.managed"] == "true" && labels["com.groundplane.kind"] == "service" &&
		labels["com.groundplane.component-id"] == "" &&
		labels["com.groundplane.environment-id"] == target.EnvironmentId &&
		labels["com.groundplane.service-id"] == target.ServiceId &&
		labels["com.groundplane.release-id"] == target.ReleaseId &&
		labels["com.groundplane.runtime-role"] == target.RuntimeRole &&
		labels["com.groundplane.slot"] == target.Slot && labels["com.docker.compose.oneoff"] != "True"
}

func replicaIdentity(labels map[string]string, target *agentpb.ServiceObservationTarget) (uint64, bool) {
	if !selected(labels, target) || labels["com.groundplane.plan-id"] != target.PlanId ||
		labels["com.groundplane.render-generation"] != strconv.FormatUint(target.RenderGeneration, 10) ||
		labels["com.docker.compose.service"] != target.ComposeName || labels["com.docker.compose.oneoff"] != "False" {
		return 0, false
	}
	value := labels["com.docker.compose.container-number"]
	ordinal, err := strconv.ParseUint(value, 10, 32)
	return ordinal, err == nil && ordinal != 0 && strconv.FormatUint(ordinal, 10) == value
}
