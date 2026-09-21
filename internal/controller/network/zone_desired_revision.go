package network

import (
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// buildZoneRemovalProjection creates the sole immutable desired revision for a
// Zone removal. It removes authored membership from DesiredServices while
// retaining the remaining desired topology.
func buildZoneRemovalProjection(
	current projectionrecord.EnvironmentComposeProjection,
	zoneID string,
	zoneName string,
	revisionID string,
	generation uint64,
) (projectionrecord.EnvironmentComposeProjection, []string, error) {
	if ids.Validate(ids.KindEnvironment, current.EnvironmentID) != nil ||
		ids.Validate(ids.KindNetwork, zoneID) != nil || zoneName == "" ||
		ids.Validate(ids.KindTask, revisionID) != nil || generation == 0 {
		return projectionrecord.EnvironmentComposeProjection{}, nil, errs.New(
			errs.KindInternal,
			"Zone removal projection input is invalid",
		)
	}

	candidate := current
	candidate.RevisionID = revisionID
	candidate.RenderGeneration = generation
	candidate.DesiredZones = make([]projectionrecord.EnvironmentZoneProjection, 0, len(current.DesiredZones)-1)
	foundDesired := false
	for _, zone := range current.DesiredZones {
		if zone.Desired.ID == zoneID && zone.Desired.Name == zoneName {
			if foundDesired {
				return projectionrecord.EnvironmentComposeProjection{}, nil, errs.New(
					errs.KindInternal,
					"Zone desired identity is duplicated",
				)
			}
			foundDesired = true
			continue
		}
		candidate.DesiredZones = append(candidate.DesiredZones, zone)
	}
	if !foundDesired {
		return projectionrecord.EnvironmentComposeProjection{}, nil, errs.New(
			errs.KindStateConflict,
			"Zone is absent from the current desired revision",
		)
	}
	candidate.DesiredServices = make([]servicerecord.EnvironmentServiceProjection, len(current.DesiredServices))
	affected := make([]string, 0)
	for index, service := range current.DesiredServices {
		candidate.DesiredServices[index] = service
		candidate.DesiredServices[index].Desired.Zones = make([]string, 0, len(service.Desired.Zones))
		removed := false
		for _, name := range service.Desired.Zones {
			if name == zoneName {
				removed = true
				continue
			}
			candidate.DesiredServices[index].Desired.Zones = append(
				candidate.DesiredServices[index].Desired.Zones,
				name,
			)
		}
		if removed {
			affected = append(affected, service.Desired.ID)
		}
	}
	sort.Strings(affected)

	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(current.ComposeArtifact, artifact); err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, nil, errs.New(
			errs.KindInternal,
			"Zone baseline artifact is corrupt",
		)
	}
	mutation := composerender.ZoneArtifactMutation{
		ZoneID: zoneID, ZoneName: zoneName,
		ArtifactID: zoneStableIDFromRevision(ids.KindConfig, revisionID),
		PlanID:     zoneStableIDFromRevision(ids.KindPlan, revisionID), RenderGeneration: generation,
	}
	mutated, err := composerender.MutateEnvironmentZoneArtifact(artifact, mutation)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, nil, err
	}
	candidate.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(mutated)
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

func zoneStableIDFromRevision(kind ids.Kind, revisionID string) string {
	if ids.Validate(ids.KindTask, revisionID) != nil {
		return ""
	}
	return string(kind) + "_" + strings.TrimPrefix(revisionID, string(ids.KindTask)+"_")
}
