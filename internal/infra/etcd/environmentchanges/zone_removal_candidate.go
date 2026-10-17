package environmentchanges

import (
	"slices"
	"sort"

	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ValidateZoneRemovalCandidate admits only the named Zone omission and removal
// of that Zone from its consumers. It is independent of Task execution records.
func ValidateZoneRemovalCandidate(source, candidate projectionrecord.EnvironmentComposeProjection, zoneID, zoneName string, consumers []string) error {
	if projectionrecord.ValidateEnvironmentComposeProjection(source) != nil ||
		projectionrecord.ValidateEnvironmentComposeProjection(candidate) != nil ||
		source.EnvironmentID != candidate.EnvironmentID {
		return errs.New(errs.KindValidationFailed, "Zone removal requires valid projections from one Environment")
	}
	expected := projectionrecord.CloneEnvironmentComposeProjection(source)
	expected.RevisionID = candidate.RevisionID
	expected.RenderGeneration = candidate.RenderGeneration
	expected.ComposeArtifact = append([]byte(nil), candidate.ComposeArtifact...)
	expected.NormalizedCompose = append([]byte(nil), candidate.NormalizedCompose...)
	expected.DesiredZones = nil
	foundZone := false
	for _, zone := range source.DesiredZones {
		if zone.Desired.ID == zoneID && zone.Desired.Name == zoneName {
			if foundZone {
				return CorruptZoneRemovalIntent()
			}
			foundZone = true
			continue
		}
		expected.DesiredZones = append(expected.DesiredZones, zone)
	}
	expected.DesiredServices = append(
		[]servicerecord.EnvironmentServiceProjection(nil),
		source.DesiredServices...)
	affected := make([]string, 0)
	for index, service := range source.DesiredServices {
		expected.DesiredServices[index] = service
		expected.DesiredServices[index].Desired.Zones = nil
		removed := false
		for _, name := range service.Desired.Zones {
			if name == zoneName {
				removed = true
				continue
			}
			expected.DesiredServices[index].Desired.Zones = append(expected.DesiredServices[index].Desired.Zones, name)
		}
		if removed {
			affected = append(affected, service.Desired.ID)
		}
	}
	sort.Strings(affected)
	if !foundZone || !sort.StringsAreSorted(consumers) ||
		!slices.Equal(affected, consumers) {
		return errs.New(errs.KindValidationFailed, "Zone removal candidate projection changed")
	}
	if !sameZoneRemovalProjection(expected, candidate) {
		return zoneRemovalProjectionChanged(expected, candidate)
	}
	return nil
}

func zoneRemovalProjectionChanged(expected, candidate projectionrecord.EnvironmentComposeProjection) error {
	switch {
	case expected.EnvironmentID != candidate.EnvironmentID || expected.RevisionID != candidate.RevisionID ||
		expected.RenderGeneration != candidate.RenderGeneration:
		return errs.New(errs.KindValidationFailed, "Zone removal candidate projection identity changed")
	case !SameServiceRemovalBytes(expected.ComposeArtifact, candidate.ComposeArtifact) ||
		!SameServiceRemovalBytes(expected.NormalizedCompose, candidate.NormalizedCompose):
		return errs.New(errs.KindValidationFailed, "Zone removal candidate projection artifact changed")
	case !SameServiceRemovalDesiredZones(expected.DesiredZones, candidate.DesiredZones):
		return errs.New(errs.KindValidationFailed, "Zone removal candidate desired Zones changed")
	case !SameServiceRemovalDesiredServices(expected.DesiredServices, candidate.DesiredServices):
		return errs.New(errs.KindValidationFailed, "Zone removal candidate desired Services changed")
	default:
		return errs.New(errs.KindValidationFailed, "Zone removal candidate projection dependencies changed")
	}
}

func sameZoneRemovalProjection(left, right projectionrecord.EnvironmentComposeProjection) bool {
	return SameServiceRemovalProjection(left, right)
}
