package blueprint

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func (planner *authoredUnitPlanner) service(name string) (composetypes.ServiceConfig, bool) {
	if service, found := planner.project.Services[name]; found {
		return service, true
	}
	service, found := planner.project.DisabledServices[name]
	return service, found
}

func (planner *authoredUnitPlanner) addUnit(
	identity projectionrecord.OwnedIdentity,
	fingerprint string,
	reads []blueprintunits.ResourceKey,
	after []blueprintunits.ResourceKey,
) {
	target := planner.targetByID(identity.ID)
	planner.units[target] = blueprintunits.Unit{
		Target: target, Fingerprint: fingerprint,
		Reads:  canonicalAuthoredUnitKeys(reads),
		Writes: []blueprintunits.ResourceKey{target},
		After:  canonicalAuthoredUnitKeys(after),
	}
}

func (planner *authoredUnitPlanner) block(identity projectionrecord.OwnedIdentity) {
	planner.blocked[planner.targetByID(identity.ID)] = true
}

func (planner *authoredUnitPlanner) targetByID(id string) blueprintunits.ResourceKey {
	for target := range planner.targets {
		if target.ID == id {
			return target
		}
	}
	return blueprintunits.ResourceKey{}
}

func (planner *authoredUnitPlanner) entryReads(spec core.EntrySpec) ([]blueprintunits.ResourceKey, error) {
	reads := make([]blueprintunits.ResourceKey, 0, 1)
	// Exposure selects immutable authored Service identities. It does not read
	// their applied runtime: Services depend on their Entries, so adding the
	// inverse read would deadlock an Entry when a prior Service is diverged.
	for _, name := range planner.entryExposureNames(spec) {
		if _, found := planner.identities[ids.KindService][name]; !found {
			return nil, planner.missingInput("Entry exposure Service", name)
		}
	}
	if spec.Source.SecretRef != "" {
		reads = append(reads, authoredResourceKey(ids.KindSecret, spec.Source.SecretRef))
	}
	if spec.Source.Fact != nil {
		attach, found := planner.identities[ids.KindAttach][spec.Source.Fact.Attach]
		if !found {
			return nil, planner.missingInput("Entry fact Attach", spec.Source.Fact.Attach)
		}
		reads = append(reads, authoredResourceKey(ids.KindAttach, attach.ID))
	}
	return canonicalAuthoredUnitKeys(reads), nil
}

func (planner *authoredUnitPlanner) entryExposureNames(spec core.EntrySpec) []string {
	if len(spec.Exposure) == 1 && spec.Exposure[0] == "all" {
		return sortedIdentityNames(planner.identities[ids.KindService])
	}
	result := slices.Clone(spec.Exposure)
	slices.Sort(result)
	return result
}

func (planner *authoredUnitPlanner) serviceDependencies(
	name string,
	service composetypes.ServiceConfig,
) ([]blueprintunits.ResourceKey, []blueprintunits.ResourceKey, error) {
	var dependencies []blueprintunits.ResourceKey
	for network := range service.Networks {
		identity, owned := planner.identities[ids.KindNetwork][network]
		if !owned {
			config, found := planner.project.Networks[network]
			if found && bool(config.External) {
				continue
			}
			return nil, nil, planner.missingInput("Service Network", network)
		}
		dependencies = append(dependencies, authoredResourceKey(ids.KindNetwork, identity.ID))
	}
	for _, mount := range service.Volumes {
		if mount.Type != composetypes.VolumeTypeVolume {
			continue
		}
		identity, found := planner.identities[ids.KindVolume][mount.Source]
		if !found {
			return nil, nil, planner.missingInput("Service Volume", mount.Source)
		}
		dependencies = append(dependencies, authoredResourceKey(ids.KindVolume, identity.ID))
	}
	for dependency := range service.DependsOn {
		identity, found := planner.identities[ids.KindService][dependency]
		if !found {
			return nil, nil, planner.missingInput("Service dependency", dependency)
		}
		dependencies = append(dependencies, authoredResourceKey(ids.KindService, identity.ID))
	}
	for dependency := range planner.input.desired.Input.ServiceExtensions[name].DependsOn {
		identity, found := planner.identities[ids.KindService][dependency]
		if !found {
			return nil, nil, planner.missingInput("Service lifecycle dependency", dependency)
		}
		dependencies = append(dependencies, authoredResourceKey(ids.KindService, identity.ID))
	}
	for _, entryName := range planner.serviceEntryNames(name) {
		identity, found := planner.identities[ids.KindEnvEntry][entryName]
		if !found {
			return nil, nil, planner.missingInput("Service Entry", entryName)
		}
		dependencies = append(dependencies, authoredResourceKey(ids.KindEnvEntry, identity.ID))
	}
	dependencies = canonicalAuthoredUnitKeys(dependencies)
	return dependencies, slices.Clone(dependencies), nil
}

func (planner *authoredUnitPlanner) serviceEntryNames(service string) []string {
	result := make([]string, 0)
	for name, spec := range planner.input.desired.Input.Entries {
		if slices.Contains(planner.entryExposureNames(spec), service) {
			result = append(result, name)
		}
	}
	slices.Sort(result)
	return result
}

func (planner *authoredUnitPlanner) serviceReleaseGroups(service string) map[string]core.ReleaseGroupSpec {
	result := make(map[string]core.ReleaseGroupSpec)
	for name, group := range planner.input.desired.Input.ReleaseGroups {
		if slices.Contains(group.Services, service) {
			result[name] = group
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func (planner *authoredUnitPlanner) componentDependencies(
	capability string,
	spec core.ComponentSpec,
) ([]blueprintunits.ResourceKey, []blueprintunits.ResourceKey, bool, error) {
	var dependencies []blueprintunits.ResourceKey
	for _, zoneID := range spec.Settings.ZoneIDs {
		if !planner.hasIdentityID(ids.KindNetwork, zoneID) {
			return nil, nil, false, planner.missingInput("Component Zone", zoneID)
		}
		dependencies = append(dependencies, authoredResourceKey(ids.KindNetwork, zoneID))
	}
	if spec.Settings.SecretID != "" {
		dependencies = append(dependencies, authoredResourceKey(ids.KindSecret, spec.Settings.SecretID))
	}
	if core.ComponentCapability(capability) == core.ComponentCapabilityHTTPRouter {
		for _, identity := range planner.identities[ids.KindRoute] {
			key := authoredResourceKey(ids.KindRoute, identity.ID)
			if planner.blocked[key] {
				return nil, nil, false, nil
			}
			dependencies = append(dependencies, key)
		}
	}
	dependencies = canonicalAuthoredUnitKeys(dependencies)
	after := make([]blueprintunits.ResourceKey, 0, len(dependencies))
	for _, dependency := range dependencies {
		if dependency.Kind != ids.KindSecret {
			after = append(after, dependency)
		}
	}
	return dependencies, canonicalAuthoredUnitKeys(after), true, nil
}

func (planner *authoredUnitPlanner) componentRoutes(capability string) []core.RouteSpec {
	if core.ComponentCapability(capability) != core.ComponentCapabilityHTTPRouter {
		return nil
	}
	return slices.Clone(planner.input.desired.Input.Routes)
}

func (planner *authoredUnitPlanner) scriptDependencies(
	spec core.ScriptSpec,
	serviceBlocked map[string]bool,
) ([]blueprintunits.ResourceKey, []blueprintunits.ResourceKey, bool, error) {
	service, found := planner.identities[ids.KindService][spec.Service]
	if !found {
		return nil, nil, false, planner.missingInput("Script Service", spec.Service)
	}
	if serviceBlocked[spec.Service] {
		return nil, nil, false, nil
	}
	dependencies := []blueprintunits.ResourceKey{authoredResourceKey(ids.KindService, service.ID)}
	if spec.Execution != nil {
		for _, grant := range spec.Execution.Volumes {
			identity, found := planner.identities[ids.KindVolume][grant.Volume]
			if !found {
				return nil, nil, false, planner.missingInput("Script Volume", grant.Volume)
			}
			dependencies = append(dependencies, authoredResourceKey(ids.KindVolume, identity.ID))
		}
		for _, name := range spec.Execution.Entries {
			identity, found := planner.identities[ids.KindEnvEntry][name]
			if !found {
				return nil, nil, false, planner.missingInput("Script Entry", name)
			}
			key := authoredResourceKey(ids.KindEnvEntry, identity.ID)
			if planner.blocked[key] {
				return nil, nil, false, nil
			}
			dependencies = append(dependencies, key)
		}
	}
	dependencies = canonicalAuthoredUnitKeys(dependencies)
	return dependencies, slices.Clone(dependencies), true, nil
}

func (planner *authoredUnitPlanner) hasIdentityID(kind ids.Kind, id string) bool {
	for _, identity := range planner.identities[kind] {
		if identity.ID == id {
			return true
		}
	}
	return false
}

func (planner *authoredUnitPlanner) serviceRuntimeFiles(service composetypes.ServiceConfig) []core.BlueprintFile {
	var exact, trees []string
	for _, value := range service.EnvFiles {
		exact = append(exact, value.Path)
	}
	exact = append(exact, service.LabelFiles...)
	for _, volume := range service.Volumes {
		if volume.Type == composetypes.VolumeTypeBind {
			trees = append(trees, volume.Source)
		}
	}
	for _, reference := range service.Configs {
		if config, found := planner.project.Configs[reference.Source]; found && config.File != "" {
			exact = append(exact, config.File)
		}
	}
	return selectAuthoredRuntimeFiles(planner.input.desired.Input.RuntimeFiles, exact, trees)
}

func (planner *authoredUnitPlanner) volumeRuntimeFiles(volume composetypes.VolumeConfig) []core.BlueprintFile {
	device := volume.DriverOpts["device"]
	if device == "" {
		return nil
	}
	return selectAuthoredRuntimeFiles(planner.input.desired.Input.RuntimeFiles, nil, []string{device})
}

func selectAuthoredRuntimeFiles(
	files []core.BlueprintFile,
	exact []string,
	trees []string,
) []core.BlueprintFile {
	exactSet := make(map[string]bool, len(exact))
	for _, value := range exact {
		exactSet[path.Clean(value)] = true
	}
	prefixes := make([]string, 0, len(trees))
	for _, value := range trees {
		prefixes = append(prefixes, strings.TrimSuffix(path.Clean(value), "/")+"/")
	}
	result := make([]core.BlueprintFile, 0)
	for _, file := range files {
		selected := exactSet[file.Path]
		for _, prefix := range prefixes {
			selected = selected || strings.HasPrefix(file.Path, prefix)
		}
		if selected {
			result = append(result, core.BlueprintFile{Path: file.Path, Content: append([]byte(nil), file.Content...)})
		}
	}
	return result
}

func authoredUnitFingerprint[T any](domain string, value T) (string, error) {
	encoded, err := json.Marshal(struct {
		Schema uint8
		Domain string
		Input  T
	}{1, domain, value})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func authoredResourceKey(kind ids.Kind, id string) blueprintunits.ResourceKey {
	return blueprintunits.ResourceKey{Kind: kind, ID: id}
}

func canonicalAuthoredUnitKeys(values []blueprintunits.ResourceKey) []blueprintunits.ResourceKey {
	values = slices.Clone(values)
	slices.SortFunc(values, compareBlueprintUnitKeys)
	return slices.Compact(values)
}

func sortedIdentityNames(values map[string]projectionrecord.OwnedIdentity) []string {
	result := make([]string, 0, len(values))
	for name := range values {
		result = append(result, name)
	}
	slices.Sort(result)
	return result
}

func (planner *authoredUnitPlanner) missingInput(kind, name string) error {
	return errs.Newf(errs.KindStateConflict, "Blueprint %s effective input %q is unavailable", kind, name)
}
