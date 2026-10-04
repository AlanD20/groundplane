package serviceobservation

import (
	"context"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingpostgresruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Backing provisioning acknowledges an Environment artifact, not a Release.
// Observe its exact owned container set and recheck that receipt after the read.
func captureBacking(
	ctx context.Context,
	releases Releases,
	service etcdstore.Versioned[servicerecord.ServiceRecord],
) (source, bool) {
	environmentID, serviceID := service.Record.EnvironmentID, service.Record.Desired.ID
	if service.Record.Desired.Adapter == "postgres:16" {
		return capturePostgres(ctx, releases, service)
	}
	applied, found, err := releases.GetAppliedProjectionAt(ctx, environmentID, service.ReadRevision)
	if err != nil || !found || applied.ReadRevision != service.ReadRevision || applied.Revision <= 0 ||
		applied.Revision > service.ReadRevision || applied.Record.EnvironmentID != environmentID {
		return source{}, false
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(applied.Record.ComposeArtifact, artifact) != nil ||
		artifact.OwnerId != environmentID || artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT {
		return source{}, false
	}
	var workload *agentpb.ComposeService
	for _, candidate := range artifact.Services {
		if candidate.ServiceId == serviceID {
			if workload != nil {
				return source{}, false
			}
			workload = candidate
		}
	}
	if workload == nil || workload.ExpectedReplicas == 0 || workload.ComposeName == "" ||
		workload.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED {
		return source{}, false
	}
	labels := make(map[string]string, len(workload.ExpectedLabels))
	for _, label := range workload.ExpectedLabels {
		if label == nil {
			return source{}, false
		}
		if _, duplicate := labels[label.Key]; duplicate {
			return source{}, false
		}
		labels[label.Key] = label.Value
	}
	planID := labels["com.groundplane.plan-id"]
	generation, err := strconv.ParseUint(labels["com.groundplane.render-generation"], 10, 64)
	if err != nil || generation == 0 || generation != applied.Record.RenderGeneration ||
		strconv.FormatUint(generation, 10) != labels["com.groundplane.render-generation"] ||
		ids.Validate(ids.KindPlan, planID) != nil || labels["com.groundplane.managed"] != "true" ||
		labels["com.groundplane.kind"] != "service" || labels["com.groundplane.environment-id"] != environmentID ||
		labels["com.groundplane.service-id"] != serviceID || labels["com.groundplane.release-id"] != "" ||
		labels["com.groundplane.runtime-role"] != "" || labels["com.groundplane.slot"] != "" ||
		labels["com.groundplane.component-id"] != "" {
		return source{}, false
	}
	return source{
		target: &agentpb.ServiceObservationTarget{EnvironmentId: environmentID, ServiceId: serviceID,
			PlanId: planID, RenderGeneration: generation, ComposeName: workload.ComposeName, RuntimeRole: "backing"},
		expected: workload.ExpectedReplicas, runtimeRevision: servicerecord.ServiceRuntimeRevision(service),
		projectionRevision: applied.Revision,
	}, true
}

func capturePostgres(ctx context.Context, releases Releases,
	service etcdstore.Versioned[servicerecord.ServiceRecord],
) (source, bool) {
	applied, found, err := releases.GetPostgresRuntimeAt(
		ctx,
		service.Record.EnvironmentID,
		service.Record.Desired.ID,
		service.ReadRevision,
	)
	if err != nil || !found || applied.ReadRevision != service.ReadRevision || applied.Revision <= 0 ||
		applied.Revision > service.ReadRevision {
		return source{}, false
	}
	_, workload, err := backingpostgresruntime.Select(applied.Record)
	if err != nil {
		return source{}, false
	}
	target := &agentpb.ServiceObservationTarget{EnvironmentId: applied.Record.EnvironmentID,
		ServiceId: applied.Record.ServiceID, ComposeName: workload.ComposeName}
	if receipt := applied.Record.Provisioning; receipt != nil {
		target.PlanId, target.RenderGeneration, target.RuntimeRole = receipt.PlanID, receipt.RenderGeneration, "backing"
	} else {
		receipt := applied.Record.Deployment
		target.PlanId, target.RenderGeneration, target.RuntimeRole = receipt.PlanID, receipt.RenderGeneration, "singleton"
		target.ReleaseId = applied.Record.DeploymentReleaseID
	}
	return source{target: target, expected: workload.ExpectedReplicas,
		runtimeRevision: servicerecord.ServiceRuntimeRevision(service), projectionRevision: applied.Revision}, true
}
