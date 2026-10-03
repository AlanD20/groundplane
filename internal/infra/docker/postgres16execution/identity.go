package postgres16execution

import (
	"context"
	"path"
	"slices"
	"strings"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/pkg/errs"
)

var protectedPaths = []string{
	"/usr/bin/env",
	postgres16protocol.HelperDirectoryPath,
	postgres16protocol.HelperPath,
	postgres16protocol.ClientGatePath,
	postgres16protocol.PGDumpPath,
	postgres16protocol.PGRestorePath,
	postgres16protocol.PSQLPath,
	postgres16protocol.StateDirectoryPath,
	postgres16protocol.ManagedReleaseManifestPath,
	"/var/run/postgresql",
	"/run/postgresql",
	"/proc",
	"/dev/null",
}

func (executor *Executor) attest(ctx context.Context, expected Container) error {
	if !validDockerID(expected.ID) || expected.Name == "" || expected.NetworkMode == "" ||
		len(
			expected.Labels,
		) == 0 || !validExpectedMounts(expected.Mounts) || !executor.matchesRuntimeImageID(expected.ImageID) {
		return errs.New(errs.KindValidationFailed, "managed PostgreSQL container authority is invalid")
	}
	image, err := executor.engine.ImageInspect(ctx, expected.ImageID)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if image.ID != expected.ImageID || image.Os != executor.manifest.OS ||
		image.Architecture != executor.manifest.Architecture ||
		(executor.manifest.Architecture == "amd64" && image.Variant != "" ||
			executor.manifest.Architecture == "arm64" && image.Variant != "" && image.Variant != "v8") ||
		image.Descriptor != nil && (!executor.matchesImageDescriptor(image.Descriptor.Digest.String()) ||
			image.Descriptor.Size <= 0) ||
		(!imageHasReference(image, executor.imageReference) && !imageHasReference(image, executor.indexReference)) {
		return errs.New(errs.KindStateConflict, "managed PostgreSQL release image changed")
	}
	inspected, err := executor.engine.ContainerInspect(ctx, expected.ID, client.ContainerInspectOptions{})
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	observed := inspected.Container
	if observed.ID != expected.ID || observed.Name != expected.Name || observed.Image != expected.ImageID ||
		observed.Platform != executor.manifest.OS || observed.Config == nil ||
		(observed.Config.Image != executor.imageReference && observed.Config.Image != executor.indexReference &&
			observed.Config.Image != expected.ImageID) ||
		observed.Config.Tty || !labelsMatch(observed.Config.Labels, expected.Labels) ||
		observed.State == nil || !observed.State.Running || observed.State.Paused ||
		observed.State.Restarting || observed.State.Dead || observed.HostConfig == nil ||
		observed.AppArmorProfile != expected.AppArmorProfile ||
		!validRootProfile(observed.HostConfig, expected) ||
		!sameMounts(observed.Mounts, expected.Mounts) {
		return errs.New(errs.KindStateConflict, "managed PostgreSQL container identity changed")
	}
	if observed.ImageManifestDescriptor != nil {
		descriptor := observed.ImageManifestDescriptor
		if descriptor.Digest.String() != executor.manifestDigest || descriptor.Size <= 0 ||
			descriptor.Platform != nil &&
				(descriptor.Platform.OS != executor.manifest.OS ||
					descriptor.Platform.Architecture != executor.manifest.Architecture) {
			return errs.New(errs.KindStateConflict, "managed PostgreSQL container manifest changed")
		}
	}
	return nil
}

func (executor *Executor) matchesImageDescriptor(digest string) bool {
	_, indexDigest, _ := strings.Cut(executor.indexReference, "@")
	return digest == executor.manifestDigest || digest == indexDigest
}

func (executor *Executor) matchesRuntimeImageID(digest string) bool {
	return digest == executor.imageID || executor.matchesImageDescriptor(digest)
}

func imageHasReference(image client.ImageInspectResult, reference string) bool {
	separator := strings.LastIndex(reference, "@sha256:")
	if separator < 0 {
		return false
	}
	digest := reference[separator+1:]
	for _, observed := range image.RepoDigests {
		if observed == reference {
			return true
		}
	}
	return image.Descriptor != nil && image.Descriptor.Digest.String() == digest
}

func validRootProfile(host *container.HostConfig, expected Container) bool {
	if host.Privileged || host.ReadonlyRootfs || string(host.NetworkMode) != expected.NetworkMode ||
		host.Runtime != expected.Runtime || string(host.UsernsMode) != expected.UsernsMode ||
		string(host.CgroupnsMode) != expected.CgroupnsMode ||
		len(host.GroupAdd) != 0 || len(host.VolumesFrom) != 0 ||
		expected.NetworkMode == "host" || strings.HasPrefix(expected.NetworkMode, "container:") ||
		string(host.PidMode) == "host" || strings.HasPrefix(string(host.PidMode), "container:") ||
		string(host.IpcMode) == "host" || strings.HasPrefix(string(host.IpcMode), "container:") ||
		!sameCapabilities(host.CapDrop, []string{"ALL"}) ||
		!sameCapabilities(host.CapAdd, postgres16protocol.ManagedRootCapabilities()) ||
		!sameSet(host.SecurityOpt, postgres16protocol.ManagedContainerSecurityOptions()) {
		return false
	}
	return true
}

func labelsMatch(observed, expected map[string]string) bool {
	if len(expected) == 0 {
		return false
	}
	for key, value := range expected {
		actual, present := observed[key]
		if !present || actual != value {
			return false
		}
	}
	return true
}

func validExpectedMounts(mounts []Mount) bool {
	seen := make(map[string]struct{}, len(mounts))
	for _, item := range mounts {
		if !validMountPath(item.Destination) || mountShadowsProtected(item.Destination) {
			return false
		}
		if _, exists := seen[item.Destination]; exists {
			return false
		}
		seen[item.Destination] = struct{}{}
	}
	return true
}

func sameMounts(observed []container.MountPoint, expected []Mount) bool {
	if len(observed) != len(expected) {
		return false
	}
	byDestination := make(map[string]Mount, len(expected))
	for _, item := range expected {
		byDestination[item.Destination] = item
	}
	seen := make(map[string]struct{}, len(observed))
	for _, item := range observed {
		if _, duplicate := seen[item.Destination]; duplicate {
			return false
		}
		seen[item.Destination] = struct{}{}
		claim, ok := byDestination[item.Destination]
		if !ok || !validMountPath(item.Destination) || mountShadowsProtected(item.Destination) ||
			claim.Type != item.Type || claim.Name != item.Name || claim.Source != item.Source ||
			claim.Driver != item.Driver || claim.Mode != item.Mode || claim.RW != item.RW ||
			claim.Propagation != item.Propagation {
			return false
		}
	}
	return true
}

func validMountPath(value string) bool {
	return strings.HasPrefix(value, "/") && value == path.Clean(value)
}

func mountShadowsProtected(destination string) bool {
	if destination == "/" {
		return true
	}
	for _, protected := range protectedPaths {
		if destination == protected || strings.HasPrefix(protected, destination+"/") ||
			protected == postgres16protocol.StateDirectoryPath &&
				strings.HasPrefix(destination, protected+"/") {
			return true
		}
	}
	return false
}

func sameSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	left = slices.Clone(left)
	right = slices.Clone(right)
	slices.Sort(left)
	slices.Sort(right)
	return slices.Equal(left, right)
}

func sameCapabilities(observed, expected []string) bool {
	// Docker prefixes inspected Linux capability names; Compose accepts short names.
	normalized := slices.Clone(observed)
	for index, value := range normalized {
		normalized[index] = strings.TrimPrefix(value, "CAP_")
	}
	return sameSet(normalized, expected)
}

func validDockerID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, letter := range value {
		if letter < '0' || letter > '9' && (letter < 'a' || letter > 'f') {
			return false
		}
	}
	return true
}
