package controller

import (
	"sort"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// ComposeResourceIdentity binds one mutable Compose key to its durable id.
type ComposeResourceIdentity struct {
	ID             string
	Name           string
	ComponentID    string
	ComponentImage *SelectedComponentImage
}

// SelectedComponentImage preserves one compiled selection across persisted rerenders.
type SelectedComponentImage struct {
	Repository  string
	IndexDigest string
	Reference   string
	Platform    componentsdk.OCIPlatform
}

// ComposeIdentitySnapshot is the durable identity projection for resources owned by one Compose project.
// Every collection is sorted by Name.
type ComposeIdentitySnapshot struct {
	Services []ComposeResourceIdentity
	Networks []ComposeResourceIdentity
	Volumes  []ComposeResourceIdentity
}

// ComposeIdentityChanges separates the next desired projection from ids that require explicit removal handling.
type ComposeIdentityChanges struct {
	Current           ComposeIdentitySnapshot
	RemovedServiceIDs []string
	RemovedNetworkIDs []string
	RemovedVolumeIDs  []string
}

// ReconcileOwnedComposeIdentities assigns stable ids to the owned resources in a parsed Compose project.
// External resources must be resolved to their owner's durable identity before entering this owned-resource stage.
func ReconcileOwnedComposeIdentities(
	project *composetypes.Project,
	previous ComposeIdentitySnapshot,
	newID func(ids.Kind) string,
) (ComposeIdentityChanges, error) {
	if project == nil {
		return ComposeIdentityChanges{}, errs.New(errs.KindValidationFailed, "compose project is required")
	}
	if newID == nil {
		return ComposeIdentityChanges{}, errs.New(errs.KindInternal, "compose identity allocator is required")
	}

	serviceNames, err := ownedServiceNames(project)
	if err != nil {
		return ComposeIdentityChanges{}, err
	}
	networkNames, err := ownedNetworkNames(project)
	if err != nil {
		return ComposeIdentityChanges{}, err
	}
	volumeNames, err := ownedVolumeNames(project)
	if err != nil {
		return ComposeIdentityChanges{}, err
	}

	services, removedServices, err := reconcileComposeResourceIdentities(
		ids.KindService,
		serviceNames,
		previous.Services,
		newID,
	)
	if err != nil {
		return ComposeIdentityChanges{}, err
	}
	networks, removedNetworks, err := reconcileComposeResourceIdentities(
		ids.KindNetwork,
		networkNames,
		previous.Networks,
		newID,
	)
	if err != nil {
		return ComposeIdentityChanges{}, err
	}
	volumes, removedVolumes, err := reconcileComposeResourceIdentities(
		ids.KindVolume,
		volumeNames,
		previous.Volumes,
		newID,
	)
	if err != nil {
		return ComposeIdentityChanges{}, err
	}

	return ComposeIdentityChanges{
		Current: ComposeIdentitySnapshot{
			Services: services,
			Networks: networks,
			Volumes:  volumes,
		},
		RemovedServiceIDs: removedServices,
		RemovedNetworkIDs: removedNetworks,
		RemovedVolumeIDs:  removedVolumes,
	}, nil
}

func ownedServiceNames(project *composetypes.Project) ([]string, error) {
	names := make([]string, 0, len(project.Services)+len(project.DisabledServices))
	seen := make(map[string]struct{}, len(project.Services)+len(project.DisabledServices))
	for name := range project.Services {
		seen[name] = struct{}{}
		names = append(names, name)
	}
	for name := range project.DisabledServices {
		if _, exists := seen[name]; exists {
			return nil, errs.New(errs.KindValidationFailed, "service is both enabled and profile-disabled")
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func ownedNetworkNames(project *composetypes.Project) ([]string, error) {
	names := make([]string, 0, len(project.Networks))
	for name, network := range project.Networks {
		if network.External {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func ownedVolumeNames(project *composetypes.Project) ([]string, error) {
	names := make([]string, 0, len(project.Volumes))
	for name, volume := range project.Volumes {
		if volume.External {
			return nil, errs.New(errs.KindInternal, "external volume reached owned identity reconciliation")
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func reconcileComposeResourceIdentities(
	kind ids.Kind,
	desiredNames []string,
	previous []ComposeResourceIdentity,
	newID func(ids.Kind) string,
) ([]ComposeResourceIdentity, []string, error) {
	byName := make(map[string]string, len(previous))
	usedIDs := make(map[string]struct{}, len(previous)+len(desiredNames))
	for _, identity := range previous {
		if identity.Name == "" || ids.Validate(kind, identity.ID) != nil {
			return nil, nil, errs.New(errs.KindInternal, "durable compose identity is invalid")
		}
		if _, exists := byName[identity.Name]; exists {
			return nil, nil, errs.New(errs.KindInternal, "durable compose identity name is duplicated")
		}
		if _, exists := usedIDs[identity.ID]; exists {
			return nil, nil, errs.New(errs.KindInternal, "durable compose resource id is duplicated")
		}
		byName[identity.Name] = identity.ID
		usedIDs[identity.ID] = struct{}{}
	}

	current := make([]ComposeResourceIdentity, 0, len(desiredNames))
	desired := make(map[string]struct{}, len(desiredNames))
	for _, name := range desiredNames {
		if name == "" {
			return nil, nil, errs.New(errs.KindValidationFailed, "compose resource name is required")
		}
		if _, exists := desired[name]; exists {
			return nil, nil, errs.New(errs.KindValidationFailed, "compose resource name is duplicated")
		}
		desired[name] = struct{}{}

		id, exists := byName[name]
		if !exists {
			id = newID(kind)
			if ids.Validate(kind, id) != nil {
				return nil, nil, errs.New(errs.KindInternal, "compose identity allocator returned an invalid id")
			}
			if _, reused := usedIDs[id]; reused {
				return nil, nil, errs.New(errs.KindInternal, "compose identity allocator reused an id")
			}
			usedIDs[id] = struct{}{}
		}
		current = append(current, ComposeResourceIdentity{ID: id, Name: name})
	}

	removed := make([]string, 0, len(previous))
	for _, identity := range previous {
		if _, exists := desired[identity.Name]; !exists {
			removed = append(removed, identity.ID)
		}
	}
	sort.Strings(removed)
	return current, removed, nil
}
