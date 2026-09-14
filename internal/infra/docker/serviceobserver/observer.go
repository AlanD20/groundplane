// Package serviceobserver reads only the Controller-selected serving workloads.
package serviceobserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
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
	ExecCreate(context.Context, string, client.ExecCreateOptions) (client.ExecCreateResult, error)
	ExecAttach(context.Context, string, client.ExecAttachOptions) (client.ExecAttachResult, error)
	ExecInspect(context.Context, string, client.ExecInspectOptions) (client.ExecInspectResult, error)
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
		if err := ctx.Err(); err != nil {
			return nil, err
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
	if target.ProxyComposeName != "" {
		proxyState, valid := observer.observeProxy(ctx, target, candidates)
		if !valid {
			return row, nil
		}
		row.ProxyState = proxyState
	}
	row.Outcome = &agentpb.ServiceObservationRow_Replicas{Replicas: counts}
	return row, nil
}

func (observer *Observer) observeProxy(
	ctx context.Context,
	target *agentpb.ServiceObservationTarget,
	candidates []container.Summary,
) (agentpb.ServiceProxyObservationState, bool) {
	containerID := ""
	for _, candidate := range candidates {
		if !proxyCandidate(candidate.Labels, target) {
			continue
		}
		if candidate.ID == "" || containerID != "" || !proxyIdentity(candidate.Labels, target) {
			return agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_UNSPECIFIED, false
		}
		containerID = candidate.ID
	}
	if containerID == "" {
		return agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MISSING, true
	}
	inspected, err := observer.engine.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
	if contextErr := ctx.Err(); contextErr != nil {
		return agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_UNSPECIFIED, false
	}
	if err != nil || inspected.Container.ID != containerID || inspected.Container.Config == nil ||
		!proxyIdentity(inspected.Container.Config.Labels, target) {
		return agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_UNSPECIFIED, false
	}
	proxyCounts := &agentpb.ServiceReplicaCounts{}
	if !countState(proxyCounts, inspected.Container.State) {
		return agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_UNSPECIFIED, false
	}
	if inspected.Container.State.Status != container.StateRunning {
		return agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_STOPPED, true
	}
	matching, valid := observer.proxyConfigMatches(ctx, containerID, target.ProxyConfigSha256)
	if !valid {
		return agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_UNSPECIFIED, false
	}
	if !matching {
		return agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_CONFIG_MISMATCH, true
	}
	return agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MATCHING, true
}

func (observer *Observer) proxyConfigMatches(ctx context.Context, containerID string, expected []byte) (bool, bool) {
	created, err := observer.engine.ExecCreate(ctx, containerID, client.ExecCreateOptions{
		TTY: true, AttachStdout: true,
		Cmd: []string{"wget", "-q", "-T", "4", "-t", "1", "-O", "-", "http://127.0.0.1:2019/config/"},
	})
	if err != nil || created.ID == "" {
		return false, false
	}
	attached, err := observer.engine.ExecAttach(ctx, created.ID, client.ExecAttachOptions{TTY: true})
	if err != nil || attached.Conn == nil {
		return false, false
	}
	defer attached.Close()
	if attached.Reader == nil {
		return false, false
	}
	stopClose := context.AfterFunc(ctx, attached.Close)
	defer stopClose()
	deadline, ok := ctx.Deadline()
	if !ok || attached.Conn.SetReadDeadline(deadline) != nil {
		return false, false
	}
	observed, err := io.ReadAll(io.LimitReader(attached.Reader, executionplan.MaximumArtifactYAMLBytes+1))
	if err != nil || len(observed) == 0 || len(observed) > executionplan.MaximumArtifactYAMLBytes || ctx.Err() != nil {
		return false, false
	}
	finished, err := observer.engine.ExecInspect(ctx, created.ID, client.ExecInspectOptions{})
	if err != nil || finished.ID != created.ID || finished.ContainerID != containerID ||
		finished.Running || finished.ExitCode != 0 {
		return false, false
	}
	canonical, err := jcs.Canonicalize(observed)
	if err != nil {
		return false, false
	}
	digest := sha256.Sum256(canonical)
	return bytes.Equal(digest[:], expected), true
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

func proxyCandidate(labels map[string]string, target *agentpb.ServiceObservationTarget) bool {
	return labels["com.groundplane.managed"] == "true" && labels["com.groundplane.kind"] == "service" &&
		labels["com.groundplane.component-id"] == "" &&
		labels["com.groundplane.environment-id"] == target.EnvironmentId &&
		labels["com.groundplane.service-id"] == target.ServiceId &&
		labels["com.groundplane.release-id"] == "" && labels["com.groundplane.runtime-role"] == "proxy" &&
		labels["com.groundplane.slot"] == "" && labels["com.docker.compose.oneoff"] != "True"
}

func proxyIdentity(labels map[string]string, target *agentpb.ServiceObservationTarget) bool {
	return proxyCandidate(labels, target) && labels["com.groundplane.plan-id"] == target.ProxyPlanId &&
		labels["com.groundplane.render-generation"] == strconv.FormatUint(target.ProxyRenderGeneration, 10) &&
		labels["com.docker.compose.service"] == target.ProxyComposeName &&
		labels["com.docker.compose.container-number"] == "1" && labels["com.docker.compose.oneoff"] == "False"
}
