package postgres16execution

import (
	"context"
	"slices"
	"strings"

	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const postgresDataPath = "/var/lib/postgresql/data"

// Selection is derived by the Agent from its sealed Backup Service fact and
// Compose artifact. It is not a Docker name, a user-selected mount, or a
// snapshot of whatever the daemon currently reports.
type Selection struct {
	Labels       map[string]string
	VolumeName   string
	VolumeLabels map[string]string
}

// ResolveContainer requires one running container with all sealed labels and
// the selected native image, and one exact named local data volume. Docker's
// dynamic IDs, host volume mountpoint, and non-security profile strings are
// then frozen for every before/after Exec inspection in this attempt.
func (executor *Executor) ResolveContainer(ctx context.Context, selection Selection) (Container, error) {
	if executor == nil || executor.engine == nil || ctx == nil || len(selection.Labels) == 0 ||
		selection.VolumeName == "" || strings.Contains(selection.VolumeName, "/") ||
		len(selection.VolumeLabels) == 0 {
		return Container{}, errs.New(errs.KindValidationFailed, "managed PostgreSQL Service selection is invalid")
	}
	keys := make([]string, 0, len(selection.Labels))
	for key := range selection.Labels {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	filters := client.Filters{}
	for _, key := range keys {
		filters = filters.Add("label", key+"="+selection.Labels[key])
	}
	listed, err := executor.engine.ContainerList(ctx, client.ContainerListOptions{Filters: filters})
	if err != nil || len(listed.Items) != 1 {
		return Container{}, errs.New(errs.KindStateConflict, "managed PostgreSQL Service container is not unique")
	}
	candidate := listed.Items[0]
	if !validDockerID(candidate.ID) || candidate.ImageID != executor.imageID ||
		!labelsMatch(candidate.Labels, selection.Labels) {
		return Container{}, errs.New(errs.KindStateConflict, "managed PostgreSQL Service identity changed")
	}
	volume, err := executor.engine.VolumeInspect(ctx, selection.VolumeName, client.VolumeInspectOptions{})
	if err != nil || volume.Volume.Name != selection.VolumeName || volume.Volume.Driver != "local" ||
		volume.Volume.Scope != "local" || !validMountPath(volume.Volume.Mountpoint) ||
		!labelsMatch(volume.Volume.Labels, selection.VolumeLabels) {
		return Container{}, errs.New(errs.KindStateConflict, "managed PostgreSQL data volume changed")
	}
	inspected, err := executor.engine.ContainerInspect(ctx, candidate.ID, client.ContainerInspectOptions{})
	if err != nil {
		return Container{}, errs.Wrap(errs.KindInternal, err)
	}
	observed := inspected.Container
	if observed.ID != candidate.ID || observed.HostConfig == nil || observed.Config == nil ||
		len(observed.Mounts) != 1 || observed.Mounts[0].Type != mount.TypeVolume ||
		observed.Mounts[0].Name != selection.VolumeName ||
		observed.Mounts[0].Source != volume.Volume.Mountpoint ||
		observed.Mounts[0].Destination != postgresDataPath || !observed.Mounts[0].RW ||
		mountShadowsProtected(observed.Mounts[0].Destination) {
		return Container{}, errs.New(errs.KindStateConflict, "managed PostgreSQL container mounts changed")
	}
	data := observed.Mounts[0]
	expected := Container{
		ID: observed.ID, Name: observed.Name,
		NetworkMode: string(observed.HostConfig.NetworkMode), Runtime: observed.HostConfig.Runtime,
		AppArmorProfile: observed.AppArmorProfile,
		UsernsMode:      string(observed.HostConfig.UsernsMode),
		CgroupnsMode:    string(observed.HostConfig.CgroupnsMode),
		Labels:          selection.Labels,
		Mounts: []Mount{{Type: data.Type, Name: data.Name, Source: data.Source,
			Destination: data.Destination, Driver: data.Driver, Mode: data.Mode,
			RW: data.RW, Propagation: data.Propagation}},
	}
	if err := executor.attest(ctx, expected); err != nil {
		return Container{}, err
	}
	return expected, nil
}
