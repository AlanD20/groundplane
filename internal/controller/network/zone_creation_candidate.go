package network

import (
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// buildZoneCreationProjection derives the one complete candidate desired
// revision. Every field is copied from the selected head except the revision
// identity, the added Zone/network, and the corresponding rendered artifacts.
func buildZoneCreationProjection(
	current etcd.EnvironmentComposeProjection,
	project etcd.ProjectRecord,
	zone zonerecord.Record,
	revisionID string,
	generation uint64,
) (etcd.EnvironmentComposeProjection, error) {
	if ids.Validate(ids.KindEnvironment, current.EnvironmentID) != nil ||
		zone.EnvironmentID != current.EnvironmentID || ids.Validate(ids.KindTask, revisionID) != nil || generation == 0 {
		return etcd.EnvironmentComposeProjection{}, errs.New(
			errs.KindInternal,
			"Zone creation projection input is invalid",
		)
	}
	for _, existing := range current.DesiredZones {
		if existing.Desired.ID == zone.Desired.ID || existing.Desired.Name == zone.Desired.Name {
			return etcd.EnvironmentComposeProjection{}, errs.New(
				errs.KindStateConflict,
				"Zone identity already exists in the current desired revision",
			)
		}
	}
	candidate := current
	candidate.RevisionID = revisionID
	candidate.RenderGeneration = generation
	candidate.DesiredZones = append(append([]etcd.EnvironmentZoneProjection(nil), current.DesiredZones...),
		etcd.EnvironmentZoneProjection(zone))
	sort.Slice(candidate.DesiredZones, func(left, right int) bool {
		return candidate.DesiredZones[left].Desired.Name < candidate.DesiredZones[right].Desired.Name
	})
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(current.ComposeArtifact, artifact); err != nil {
		return etcd.EnvironmentComposeProjection{}, errs.New(errs.KindInternal, "Zone baseline artifact is corrupt")
	}
	addition := controller.ZoneArtifactAddition{
		Zone: zone.Desired, ProjectID: project.ID, TenantID: project.TenantID,
		ArtifactID: zoneStableIDFromRevision(ids.KindConfig, revisionID),
		PlanID:     zoneStableIDFromRevision(ids.KindPlan, revisionID), RenderGeneration: generation,
	}
	mutated, err := controller.AddEnvironmentZoneArtifact(artifact, addition)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, err
	}
	candidate.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(mutated)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, errs.Wrap(errs.KindInternal, err)
	}
	normalized, err := controller.NormalizedEnvironmentArtifact(current)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, err
	}
	normalized, err = controller.AddEnvironmentZoneArtifact(normalized, addition)
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, err
	}
	candidate.NormalizedCompose = append([]byte(nil), normalized.GetCanonicalYaml()...)
	return candidate, nil
}
