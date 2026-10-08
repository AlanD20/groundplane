package softwarestaging

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/registryimages"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// StageAgent imports exactly the verified preparation before its update Task
// can replace the predecessor. Replays verify existing content, never a tag.
func (stager *Stager) StageAgent(ctx context.Context, artifact preparation.Artifact) error {
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
	_, err = registry.Fetch(ctx, plan, func(imagefetch.Progress) error { return ctx.Err() })
	return err
}
