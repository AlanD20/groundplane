package registryimages

import (
	"context"
	"errors"
	"time"

	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
)

// InspectSoftware reads only the immutable local configuration captured by an
// accepted runtime. RepoTags and registry responses cannot provide its version.
func (Local) InspectSoftware(
	ctx context.Context,
	accepted buildversion.ContainerIdentity,
) (buildversion.ContainerIdentity, error) {
	if err := accepted.Validate(); err != nil {
		return buildversion.ContainerIdentity{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return buildversion.ContainerIdentity{}, errs.Wrap(errs.KindInternal, err)
	}
	image, inspectErr := engine.ImageInspect(ctx, accepted.ConfigDigest)
	if err := errors.Join(inspectErr, engine.Close()); err != nil {
		return buildversion.ContainerIdentity{}, errs.Wrap(errs.KindRequestUnavailable, err)
	}
	if image.ID != accepted.ConfigDigest {
		return buildversion.ContainerIdentity{}, errs.New(
			errs.KindStateConflict,
			"installed image configuration identity changed",
		)
	}
	// Label-free upstream images are valid installations with unknown provenance.
	// Only GP-labelled images have a source-kind interpretation; an upstream
	// revision label alone does not make that image a Groundplane source build.
	identity := buildversion.ContainerIdentity{
		Image:        accepted.Image,
		ConfigDigest: accepted.ConfigDigest,
		SelectedTag:  accepted.SelectedTag,
	}
	identity.Version = image.Config.Labels["com.groundplane.version"]
	if identity.Version == "" {
		identity.Version = image.Config.Labels["org.opencontainers.image.version"]
	}
	if kind := image.Config.Labels["com.groundplane.source-kind"]; kind != "" {
		identity.Source = buildversion.BuildProvenance{
			Kind:   kind,
			Commit: image.Config.Labels["org.opencontainers.image.revision"],
		}
	}
	if err := identity.Validate(); err != nil {
		return buildversion.ContainerIdentity{}, errs.Wrap(errs.KindStateConflict, err)
	}
	return identity, nil
}
