package blueprintrelease

import (
	"bytes"
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"slices"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type predecessorSnapshot struct {
	planning       etcd.ReleasePlanningService
	serving        *domain.Intent
	historical     etcdstore.Versioned[etcd.ReleaseRenderInput]
	applied        etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	appliedPresent bool
	native         etcd.BlueprintNativePredecessorCapture
}

func (service *Service) preparePredecessor(
	ctx context.Context,
	input PrepareInput,
	candidate blueprints.EnvironmentBlueprintServiceChange,
	render *etcd.ReleaseRenderInput,
	intent *domain.Intent,
) error {
	if candidate.Current == nil {
		if _, exists := input.Workloads.predecessors[candidate.Record.Desired.Name]; exists {
			return errs.New(errs.KindStateConflict, "Blueprint predecessor Service disappeared after preflight")
		}
		return nil
	}
	captured, exists := input.Workloads.predecessors[candidate.Record.Desired.Name]
	if !exists {
		return errs.New(errs.KindStateConflict, "Blueprint predecessor was not captured by image preflight")
	}
	scope, err := service.ledger.LoadPlanningScopeAtRevision(
		ctx,
		input.Environment.Record.ID,
		candidate.Current.ReadRevision,
	)
	if err != nil {
		return err
	}
	planning, err := service.ledger.LoadPlanningServices(ctx, scope, []string{candidate.Record.Desired.ID})
	if err != nil {
		return err
	}
	applied, exists, err := service.ledger.GetPlanningAppliedProjection(ctx, scope)
	if err != nil {
		return err
	}
	if err := captured.matches(planning[0], applied, exists); err != nil {
		return err
	}
	intent.PriorServingReleaseID = captured.planning.Projection.ServingReleaseID
	intent.PriorSuccessfulReleaseID = captured.planning.Projection.CurrentSuccessfulReleaseID
	if captured.serving == nil {
		return nil
	}
	render.PriorRuntime = &taskassignments.ReleaseNativePredecessorAuthority{
		ServiceID: captured.native.ServiceID, CurrentArtifact: slices.Clone(captured.native.CurrentArtifact),
		RetainedPriorArtifact: slices.Clone(captured.native.RetainedPriorArtifact),
	}
	return bindPredecessor(
		render,
		intent,
		*captured.serving,
		captured.historical.Record,
		projectionrecord.EnvironmentComposeProjection{ComposeArtifact: captured.native.CurrentArtifact},
	)
}

func (service *Service) capturePredecessor(
	ctx context.Context,
	scope etcd.ReleasePlanningScope,
	planning etcd.ReleasePlanningService,
) (predecessorSnapshot, error) {
	captured := predecessorSnapshot{planning: planning}
	serving, exists, err := service.ledger.GetPlanningServingIntent(ctx, scope, planning)
	if err != nil {
		return predecessorSnapshot{}, err
	}
	captured.applied, captured.appliedPresent, err = service.ledger.GetPlanningAppliedProjection(ctx, scope)
	if err != nil {
		return predecessorSnapshot{}, err
	}
	if exists {
		if !captured.appliedPresent {
			return predecessorSnapshot{}, errs.New(
				errs.KindStateConflict,
				"Blueprint serving predecessor has no acknowledged artifact",
			)
		}
		captured.serving = &serving
		captured.historical, err = service.ledger.GetReleaseRenderInputAt(ctx, serving.ID, scope.ReadRevision)
		if err != nil {
			return predecessorSnapshot{}, err
		}
	}
	return service.captureNativePredecessor(ctx, scope, captured)
}

func (captured predecessorSnapshot) matches(
	planning etcd.ReleasePlanningService,
	applied etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	present bool,
) error {
	if captured.planning.Service.Record.Desired.ID != planning.Service.Record.Desired.ID ||
		captured.planning.ProjectionRevision != planning.ProjectionRevision || captured.planning.Projection != planning.Projection ||
		captured.appliedPresent != present || captured.applied.Revision != applied.Revision ||
		!bytes.Equal(captured.applied.Record.ComposeArtifact, applied.Record.ComposeArtifact) {
		return errs.New(errs.KindStateConflict, "Blueprint predecessor changed after image preflight")
	}
	return nil
}

func bindPredecessor(
	render *etcd.ReleaseRenderInput,
	intent *domain.Intent,
	serving domain.Intent,
	historical etcd.ReleaseRenderInput,
	applied projectionrecord.EnvironmentComposeProjection,
) error {
	if serving.ID != intent.PriorServingReleaseID || serving.ServiceID != render.ServiceID ||
		serving.EnvironmentID != render.EnvironmentID ||
		historical.ReleaseID != serving.ID ||
		historical.ServiceID != serving.ServiceID ||
		historical.EnvironmentID != serving.EnvironmentID ||
		historical.CandidateWorkload != serving.CandidateWorkload ||
		historical.Strategy != serving.Strategy ||
		historical.Slot != serving.Slot {
		return errs.New(errs.KindStateConflict, "Blueprint historical predecessor identity diverges")
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(applied.ComposeArtifact, artifact); err != nil ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != render.EnvironmentID ||
		artifact.GetArtifactId() == "" {
		return errs.New(errs.KindStateConflict, "Blueprint acknowledged predecessor artifact is invalid")
	}
	matched := false
	for _, previous := range artifact.Services {
		if previous.GetServiceId() != serving.ServiceID {
			continue
		}
		if historical.CandidateTarget == domain.WorkloadSingleton {
			if previous.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
				continue
			}
		} else if previous.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT || previous.GetSlot() != string(historical.CandidateTarget) {
			continue
		}
		if matched || previous.GetImageReference() != serving.CandidateWorkload.LocalImageID ||
			previous.GetExpectedReplicas() != serving.CandidateWorkload.ReplicaCount {
			return errs.New(errs.KindStateConflict, "Blueprint acknowledged predecessor workload diverges")
		}
		for _, label := range previous.GetExpectedLabels() {
			if label.GetKey() == "com.groundplane.release-id" && label.GetValue() == serving.ID {
				matched = true
			}
		}
	}
	if !matched {
		return errs.New(errs.KindStateConflict, "Blueprint acknowledged predecessor Release is absent")
	}
	workload := serving.CandidateWorkload
	render.PriorWorkload, render.PriorArtifactID = &workload, artifact.ArtifactId
	render.PriorStrategy, render.PriorSlot, render.PriorTarget = serving.Strategy, serving.Slot, historical.CandidateTarget
	if len(historical.ProxyPorts) != 0 {
		render.ProxyPorts = slices.Clone(historical.ProxyPorts)
		render.PriorProxyGeneration, render.PriorProxyDigest = historical.ProxyGeneration, historical.ProxyConfigDigest
		if historical.ProxyImage == nil || historical.ProxyImage.Validate() != nil {
			return errs.New(errs.KindStateConflict, "Blueprint historical proxy image authority is absent")
		}
		image := *historical.ProxyImage
		render.ProxyImage = &image
	}
	return nil
}
