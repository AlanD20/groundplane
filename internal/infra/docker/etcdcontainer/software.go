package etcdcontainer

import (
	"context"

	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// InstalledSoftware reads the owned container, not the image selected for the
// next bootstrap. The immutable image configuration owns any version label.
func (manager *Manager) InstalledSoftware(ctx context.Context) (*buildversion.ContainerIdentity, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	container, found, err := manager.inspect(ctx)
	if err != nil || !found {
		return nil, err
	}
	if !owned(container) {
		return nil, errs.New(errs.KindStateConflict, "etcd software observation found an unowned container")
	}
	identity := &buildversion.ContainerIdentity{Image: container.Config.Image, ConfigDigest: container.Image,
		SelectedTag: container.Config.Labels[labelImageSelectedTag]}
	if err := identity.Validate(); err != nil {
		return nil, errs.Wrap(errs.KindStateConflict, err)
	}
	image, err := manager.client.ImageInspect(ctx, container.Image)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		// Optional image labels do not invalidate the owned container's captured
		// reference, configuration id or selected tag.
		return identity, nil
	}
	if image.ID != container.Image {
		return nil, errs.New(errs.KindStateConflict, "etcd image configuration identity changed")
	}
	label := image.Config.Labels["org.opencontainers.image.version"]
	if (buildversion.BuildIdentity{Version: label}).Validate() == nil {
		identity.Version = label
	}
	return identity, nil
}
