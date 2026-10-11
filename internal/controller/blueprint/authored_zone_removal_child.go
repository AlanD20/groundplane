package blueprint

import (
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (service *Service) prepareAuthoredZoneRemovalChild(
	ctx context.Context, parent etcd.TaskRecord, unit blueprintunits.Unit, timeoutSeconds int64,
) (blueprintcoordinator.PreparedChild, error) {
	if !unit.Removal || unit.Target.Kind != ids.KindNetwork || service.plans == nil {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindValidationFailed, "Blueprint Zone removal child identity is invalid",
		)
	}
	applied, found, err := service.repository.GetEnvironmentAppliedComposeProjection(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if !found {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Zone applied projection is unavailable",
		)
	}
	zone, err := appliedZoneRemovalSource(applied, unit.Target.ID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if zone.Record.EnvironmentID != parent.Owner.EnvironmentID {
		return blueprintcoordinator.PreparedChild{}, errs.New(errs.KindStateConflict, "Blueprint Zone owner changed")
	}
	head, found, err := service.repository.GetEnvironmentBlueprintHead(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if !found || head.Record.RevisionID != parent.ID {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Zone removal parent is no longer current",
		)
	}
	createdAt := service.now().UTC()
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), PlanID: ids.New(ids.KindPlan),
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem, Executor: taskjournal.TaskExecutorAgent,
		Type: taskjournal.TaskRemove, Target: unit.Target.ID,
		Params:         map[string]string{taskjournal.TaskBlueprintParentParam: parent.ID},
		TimeoutSeconds: timeoutSeconds, Status: taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	candidate, affected, err := buildAuthoredZoneRemovalProjection(
		applied.Record,
		zone.Record.Desired.ID,
		zone.Record.Desired.Name,
		parent.ID,
		uint64(parent.RenderGeneration),
		ids.New(ids.KindConfig),
		child.PlanID,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	intent, err := environmentchanges.NewAuthoredZoneRemovalIntent(
		child.OperationID, child.ID, zone, applied, head.Revision, parent.ID,
		uint64(parent.RenderGeneration), candidate, affected, createdAt,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	child, err = service.plans.PrepareZoneRemovalTask(ctx, child, intent, ids.New(ids.KindStep))
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	publication, err := etcd.NewBlueprintZoneRemovalPublication(child, intent)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{Task: child, ZoneRemovalPublication: publication}, nil
}

func appliedZoneRemovalSource(
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	zoneID string,
) (etcdstore.Versioned[zonerecord.Record], error) {
	var selected *etcdstore.Versioned[zonerecord.Record]
	for _, desired := range projection.Record.DesiredZones {
		if desired.Desired.ID != zoneID {
			continue
		}
		if selected != nil {
			return etcdstore.Versioned[zonerecord.Record]{}, projectionrecord.CorruptEnvironmentComposeProjection()
		}
		joined, err := environmentqueries.JoinZone(projection, desired)
		if err != nil {
			return etcdstore.Versioned[zonerecord.Record]{}, err
		}
		selected = &joined
	}
	if selected == nil {
		return etcdstore.Versioned[zonerecord.Record]{}, errs.New(
			errs.KindStateConflict, "Blueprint Zone is absent from the applied projection",
		)
	}
	return *selected, nil
}

func buildAuthoredZoneRemovalProjection(
	current projectionrecord.EnvironmentComposeProjection,
	zoneID, zoneName, revisionID string,
	renderGeneration uint64,
	artifactID, planID string,
) (projectionrecord.EnvironmentComposeProjection, []string, error) {
	candidate := projectionrecord.CloneEnvironmentComposeProjection(current)
	candidate.RevisionID, candidate.RenderGeneration = revisionID, renderGeneration
	candidate.DesiredZones = nil
	removed := false
	for _, desired := range current.DesiredZones {
		if desired.Desired.ID == zoneID && desired.Desired.Name == zoneName {
			if removed {
				return projectionrecord.EnvironmentComposeProjection{}, nil,
					projectionrecord.CorruptEnvironmentComposeProjection()
			}
			removed = true
			continue
		}
		candidate.DesiredZones = append(candidate.DesiredZones, desired)
	}
	if !removed {
		return projectionrecord.EnvironmentComposeProjection{}, nil, errs.New(
			errs.KindStateConflict, "Blueprint Zone is absent from the applied projection",
		)
	}
	candidate.DesiredServices = make([]servicerecord.EnvironmentServiceProjection, len(current.DesiredServices))
	affected := make([]string, 0)
	for index, desired := range current.DesiredServices {
		candidate.DesiredServices[index] = desired
		candidate.DesiredServices[index].Desired.Zones = nil
		removedMembership := false
		for _, name := range desired.Desired.Zones {
			if name == zoneName {
				removedMembership = true
				continue
			}
			candidate.DesiredServices[index].Desired.Zones = append(
				candidate.DesiredServices[index].Desired.Zones, name,
			)
		}
		if removedMembership {
			affected = append(affected, desired.Desired.ID)
		}
	}
	sort.Strings(affected)
	mutation := composerender.ZoneArtifactMutation{
		ZoneID: zoneID, ZoneName: zoneName, ArtifactID: artifactID,
		PlanID: planID, RenderGeneration: renderGeneration,
	}
	artifact := &agentpb.ComposeArtifact{}
	if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(current.ComposeArtifact, artifact) != nil {
		return projectionrecord.EnvironmentComposeProjection{}, nil, errs.New(
			errs.KindInternal, "Blueprint Zone applied artifact is corrupt",
		)
	}
	artifact, err := composerender.MutateEnvironmentZoneArtifact(artifact, mutation)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, nil, err
	}
	candidate.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	normalized, err := composerender.NormalizedEnvironmentArtifact(current)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, nil, err
	}
	normalized, err = composerender.MutateEnvironmentZoneArtifact(normalized, mutation)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, nil, err
	}
	candidate.NormalizedCompose = append([]byte(nil), normalized.GetCanonicalYaml()...)
	return candidate, affected, nil
}
