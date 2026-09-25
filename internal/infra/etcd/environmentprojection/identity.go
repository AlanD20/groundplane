package environmentprojection

import (
	"slices"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/core"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// OwnedIdentity records stable resource IDs at desired-head publication. It is
// derived identity authority, not operator input or an applied runtime fact.
// A successor Apply can use it while this revision is still rendering.
type OwnedIdentity struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	Slug              string `json:"slug,omitempty"`
	ValueGenerationID string `json:"value_generation_id,omitempty"`
	BirthRevisionID   string `json:"birth_revision_id"`
}

type EnvironmentOwnedIdentities struct {
	EnvironmentID    string          `json:"environment_id"`
	RevisionID       string          `json:"revision_id"`
	RenderGeneration uint64          `json:"render_generation"`
	Services         []OwnedIdentity `json:"services,omitempty"`
	Networks         []OwnedIdentity `json:"networks,omitempty"`
	Volumes          []OwnedIdentity `json:"volumes,omitempty"`
	Entries          []OwnedIdentity `json:"entries,omitempty"`
	Routes           []OwnedIdentity `json:"routes,omitempty"`
	Attaches         []OwnedIdentity `json:"attaches,omitempty"`
	Components       []OwnedIdentity `json:"components,omitempty"`
	Scripts          []OwnedIdentity `json:"scripts,omitempty"`
}

// OwnedIdentitiesFromProjection captures the IDs already assigned during
// admission. It does not claim the projection has been applied to the host.
func OwnedIdentitiesFromProjection(projection EnvironmentComposeProjection) (EnvironmentOwnedIdentities, error) {
	value := EnvironmentOwnedIdentities{
		EnvironmentID: projection.EnvironmentID, RevisionID: projection.RevisionID,
		RenderGeneration: projection.RenderGeneration,
		Services:         make([]OwnedIdentity, len(projection.DesiredServices)),
		Networks:         make([]OwnedIdentity, len(projection.DesiredZones)),
		Volumes:          make([]OwnedIdentity, len(projection.Volumes)),
		Entries:          make([]OwnedIdentity, len(projection.Entries)),
		Routes:           make([]OwnedIdentity, len(projection.DesiredRoutes)),
		Components:       make([]OwnedIdentity, len(projection.Components)),
	}
	for index, service := range projection.DesiredServices {
		if service.EnvironmentID != projection.EnvironmentID {
			return EnvironmentOwnedIdentities{}, errs.New(errs.KindValidationFailed, "Service identity has wrong Environment")
		}
		value.Services[index] = OwnedIdentity{
			ID: service.Desired.ID, Name: service.Desired.Name, BirthRevisionID: projection.RevisionID,
		}
	}
	for index, network := range projection.DesiredZones {
		if network.EnvironmentID != projection.EnvironmentID {
			return EnvironmentOwnedIdentities{}, errs.New(errs.KindValidationFailed, "Zone identity has wrong Environment")
		}
		value.Networks[index] = OwnedIdentity{
			ID: network.Desired.ID, Name: network.Desired.Name, BirthRevisionID: projection.RevisionID,
		}
	}
	for index, volume := range projection.Volumes {
		value.Volumes[index] = OwnedIdentity{
			ID: volume.ID, Name: volume.Key, Slug: volume.Slug, BirthRevisionID: projection.RevisionID,
		}
	}
	for index, entry := range projection.Entries {
		if entry.EnvironmentID != projection.EnvironmentID {
			return EnvironmentOwnedIdentities{}, errs.New(errs.KindValidationFailed, "Entry identity has wrong Environment")
		}
		value.Entries[index] = OwnedIdentity{
			ID: entry.Entry.ID, Name: entryrecord.BlueprintIdentityKey(entry),
			BirthRevisionID:   projection.RevisionID,
			ValueGenerationID: entry.CurrentValueGenerationID,
		}
	}
	for index, route := range projection.DesiredRoutes {
		if route.EnvironmentID != projection.EnvironmentID {
			return EnvironmentOwnedIdentities{}, errs.New(errs.KindValidationFailed, "Route identity has wrong Environment")
		}
		value.Routes[index] = OwnedIdentity{
			ID:              route.Desired.ID,
			Name:            RouteIdentityName(route.Desired.Host, route.Desired.Path),
			BirthRevisionID: projection.RevisionID,
		}
	}
	for index, component := range projection.Components {
		if component.Desired.Owner != core.ComponentOwnerEnvironment ||
			component.Desired.OwnerID != projection.EnvironmentID {
			return EnvironmentOwnedIdentities{}, errs.New(errs.KindValidationFailed, "Component identity has wrong Environment")
		}
		value.Components[index] = OwnedIdentity{
			ID: component.Desired.ID, Name: string(component.Desired.Kind), BirthRevisionID: projection.RevisionID,
		}
	}
	sort.Slice(value.Services, func(i, j int) bool { return value.Services[i].Name < value.Services[j].Name })
	sort.Slice(value.Networks, func(i, j int) bool { return value.Networks[i].Name < value.Networks[j].Name })
	sort.Slice(value.Volumes, func(i, j int) bool { return value.Volumes[i].Name < value.Volumes[j].Name })
	sort.Slice(value.Entries, func(i, j int) bool { return value.Entries[i].Name < value.Entries[j].Name })
	sort.Slice(value.Routes, func(i, j int) bool { return value.Routes[i].Name < value.Routes[j].Name })
	sort.Slice(value.Components, func(i, j int) bool { return value.Components[i].Name < value.Components[j].Name })
	if err := ValidateEnvironmentOwnedIdentities(value); err != nil {
		return EnvironmentOwnedIdentities{}, err
	}
	return value, nil
}

func CloneEnvironmentOwnedIdentities(source EnvironmentOwnedIdentities) EnvironmentOwnedIdentities {
	clone := source
	clone.Services = append([]OwnedIdentity(nil), source.Services...)
	clone.Networks = append([]OwnedIdentity(nil), source.Networks...)
	clone.Volumes = append([]OwnedIdentity(nil), source.Volumes...)
	clone.Entries = append([]OwnedIdentity(nil), source.Entries...)
	clone.Routes = append([]OwnedIdentity(nil), source.Routes...)
	clone.Attaches = append([]OwnedIdentity(nil), source.Attaches...)
	clone.Components = append([]OwnedIdentity(nil), source.Components...)
	clone.Scripts = append([]OwnedIdentity(nil), source.Scripts...)
	return clone
}

// RetainOwnedIdentityBirths carries creation evidence across a new desired
// revision. A missing predecessor is valid only for a first desired head;
// callers must establish that from the fenced head read, not from an absent
// identity key alone.
func RetainOwnedIdentityBirths(
	current EnvironmentOwnedIdentities,
	previous EnvironmentOwnedIdentities,
) (EnvironmentOwnedIdentities, error) {
	if err := ValidateEnvironmentOwnedIdentities(current); err != nil {
		return EnvironmentOwnedIdentities{}, err
	}
	if err := ValidateEnvironmentOwnedIdentities(previous); err != nil {
		return EnvironmentOwnedIdentities{}, err
	}
	if current.EnvironmentID != previous.EnvironmentID ||
		current.RenderGeneration <= previous.RenderGeneration {
		return EnvironmentOwnedIdentities{}, errs.New(errs.KindValidationFailed, "Environment owned identity predecessor is invalid")
	}
	result := CloneEnvironmentOwnedIdentities(current)
	retainOwnedBirths(result.Services, previous.Services)
	retainOwnedBirths(result.Networks, previous.Networks)
	retainOwnedBirths(result.Volumes, previous.Volumes)
	retainOwnedBirths(result.Entries, previous.Entries)
	retainOwnedBirths(result.Routes, previous.Routes)
	retainOwnedBirths(result.Attaches, previous.Attaches)
	retainOwnedBirths(result.Components, previous.Components)
	retainOwnedBirths(result.Scripts, previous.Scripts)
	if err := ValidateEnvironmentOwnedIdentities(result); err != nil {
		return EnvironmentOwnedIdentities{}, err
	}
	return result, nil
}

func retainOwnedBirths(current, previous []OwnedIdentity) {
	prior := make(map[string]string, len(previous))
	for _, identity := range previous {
		prior[identity.ID] = identity.BirthRevisionID
	}
	for index := range current {
		if birth, found := prior[current[index].ID]; found {
			current[index].BirthRevisionID = birth
		}
	}
}

func ValidateEnvironmentOwnedIdentities(value EnvironmentOwnedIdentities) error {
	if recordcodec.ValidateID(ids.KindEnvironment, value.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, value.RevisionID) != nil || value.RenderGeneration == 0 ||
		!validOwnedIdentities(value.Services, ids.KindService) ||
		!validOwnedIdentities(value.Networks, ids.KindNetwork) ||
		!validOwnedIdentities(value.Volumes, ids.KindVolume) ||
		!validOwnedIdentities(value.Entries, ids.KindEnvEntry) ||
		!validOwnedIdentities(value.Routes, ids.KindRoute) ||
		!validOwnedIdentities(value.Attaches, ids.KindAttach) ||
		!validOwnedIdentities(value.Components, ids.KindComponent) ||
		!validOwnedIdentities(value.Scripts, ids.KindScript) {
		return errs.New(errs.KindValidationFailed, "Environment owned identities are invalid")
	}
	return nil
}

func RouteIdentityName(host, path string) string {
	if path == "" {
		path = "/"
	}
	return host + "\x00" + path
}

func validOwnedIdentities(values []OwnedIdentity, kind ids.Kind) bool {
	if len(values) > 512 || !slices.IsSortedFunc(values, func(a, b OwnedIdentity) int {
		return compareOwnedIdentityNames(a.Name, b.Name)
	}) {
		return false
	}
	seenIDs := make(map[string]struct{}, len(values))
	seenSlugs := make(map[string]struct{}, len(values))
	for index, value := range values {
		if value.Name == "" || ids.Validate(kind, value.ID) != nil ||
			ids.Validate(ids.KindTask, value.BirthRevisionID) != nil ||
			index > 0 && values[index-1].Name == value.Name {
			return false
		}
		if kind == ids.KindVolume {
			if slug.Validate("Environment Volume slug", value.Slug) != nil {
				return false
			}
			if _, duplicate := seenSlugs[value.Slug]; duplicate {
				return false
			}
			seenSlugs[value.Slug] = struct{}{}
		} else if value.Slug != "" {
			return false
		}
		if kind == ids.KindEnvEntry {
			if ids.Validate(ids.KindConfig, value.ValueGenerationID) != nil {
				return false
			}
		} else if value.ValueGenerationID != "" {
			return false
		}
		if _, duplicate := seenIDs[value.ID]; duplicate {
			return false
		}
		seenIDs[value.ID] = struct{}{}
	}
	return true
}

func compareOwnedIdentityNames(left, right string) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func EncodeEnvironmentOwnedIdentities(value EnvironmentOwnedIdentities) ([]byte, error) {
	if err := ValidateEnvironmentOwnedIdentities(value); err != nil {
		return nil, err
	}
	return recordcodec.Encode("environment-owned-identities", value)
}

func DecodeEnvironmentOwnedIdentities(encoded []byte) (EnvironmentOwnedIdentities, error) {
	value, err := recordcodec.Decode[EnvironmentOwnedIdentities](encoded, "environment-owned-identities")
	if err != nil || ValidateEnvironmentOwnedIdentities(value) != nil {
		return EnvironmentOwnedIdentities{}, errs.New(errs.KindInternal, "Environment owned identities are corrupt")
	}
	return value, nil
}
