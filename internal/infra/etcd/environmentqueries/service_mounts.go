package environmentqueries

import (
	"github.com/AlanD20/groundplane/internal/core"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

// ServicesWithVolumeMounts joins native Compose mounts for scoped reads and
// captured Release planning without rediscovering the current desired head.
func ServicesWithVolumeMounts(
	projection projectionrecord.EnvironmentComposeProjection,
) []servicerecord.EnvironmentServiceProjection {
	services := append([]servicerecord.EnvironmentServiceProjection(nil), projection.DesiredServices...)
	for index := range services {
		desired := &services[index].Desired
		mounts := make([]core.Mount, 0)
		for _, mount := range desired.Mounts {
			if mount.File != "" {
				mounts = append(mounts, mount)
			}
		}
		for _, mount := range projection.VolumeMounts {
			if mount.ServiceID == desired.ID {
				mounts = append(mounts, core.Mount{Volume: mount.VolumeID, Mount: mount.Target, RO: mount.ReadOnly})
			}
		}
		desired.Mounts = mounts
	}
	return services
}
