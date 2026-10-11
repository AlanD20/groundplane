package runner

import (
	"context"

	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
)

func installedRunnerSoftware(
	ctx context.Context,
	engine *client.Client,
	imageID, imageRef string,
) (buildversion.ContainerIdentity, error) {
	inspected, err := engine.ImageInspect(ctx, imageRef)
	if err != nil {
		return buildversion.ContainerIdentity{}, dockerError(ctx, "inspect installed Runner image", err)
	}
	if inspected.ID != imageID {
		return buildversion.ContainerIdentity{}, errs.New(
			errs.KindStateConflict,
			"Runner image configuration identity changed",
		)
	}
	identity := buildversion.ContainerIdentity{
		Image: imageRef, ConfigDigest: imageID,
		Version: inspected.Config.Labels["com.groundplane.version"],
		Source: buildversion.BuildProvenance{
			Kind:   inspected.Config.Labels["com.groundplane.source-kind"],
			Commit: inspected.Config.Labels["org.opencontainers.image.revision"],
		},
	}
	if err := identity.Validate(); err != nil {
		return buildversion.ContainerIdentity{}, err
	}
	return identity, nil
}
