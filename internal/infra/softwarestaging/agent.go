package softwarestaging

import (
	"context"
	"fmt"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/agentimagemetadata"
	"github.com/AlanD20/groundplane/internal/infra/registryimages"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// StageAgent imports exactly the verified preparation before its update Task
// can replace the predecessor. Replays verify existing content, never a tag.
func (stager *Stager) StageAgent(ctx context.Context, artifact preparation.Artifact, channel int, owner string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	registry := registryimages.Local{}
	plan, err := registry.Resolve(ctx, artifact.Reference)
	if err != nil {
		return err
	}
	if plan.Reference() != artifact.Reference || plan.ManifestDigest != artifact.ManifestDigest ||
		plan.ConfigDigest != artifact.ConfigDigest || artifact.Platform.OS != "linux" ||
		plan.Architecture != artifact.Platform.Architecture {
		return errs.New(errs.KindStateConflict, "prepared Agent registry identity changed")
	}
	if _, err = registry.Fetch(ctx, plan, func(imagefetch.Progress) error { return ctx.Err() }); err != nil {
		return err
	}
	metadata, err := agentimagemetadata.Read(ctx, artifact.Reference, owner)
	if err != nil {
		return err
	}
	if metadata.ChannelSchema != channel {
		return errs.New(
			errs.KindSoftwareProtocolIncompatible,
			fmt.Sprintf(
				"Agent channel format %d does not match Controller format %d; prepare Both with matching builds",
				metadata.ChannelSchema,
				channel,
			),
		)
	}
	return nil
}
