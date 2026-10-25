package backingservices

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backingcatalog"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/pkg/errs"
	dockerreference "github.com/distribution/reference"
)

type creationImageRegistry interface {
	Resolve(context.Context, string) (imagefetch.Plan, error)
}

type creationImage struct {
	reference    string
	localID      string
	architecture string
	variant      string
}

// sealCreationImage captures exact host bytes before publishing native runtime.
// Desired state keeps the authored family tag; execution never resolves it again.
func (service *CreationService) sealCreationImage(ctx context.Context, reference string) (creationImage, error) {
	if service.agents == nil || service.images == nil || service.imageRegistry == nil {
		return creationImage{}, errs.New(errs.KindInternal, "Backing creation image authority is unavailable")
	}
	agent, err := service.agents.GetSingleton(ctx)
	if err != nil {
		return creationImage{}, err
	}
	// The managed catalog's tag constrains its server line; the digest alone
	// selects delivery bytes. Do not pass both selectors to the image resolver.
	authored := reference
	if backingcatalog.ValidPinnedImage(reference) {
		named, err := dockerreference.ParseNormalizedNamed(reference)
		if err != nil {
			return creationImage{}, errs.Wrap(errs.KindValidationFailed, err)
		}
		pinned, ok := named.(dockerreference.Canonical)
		if !ok {
			return creationImage{}, errs.New(errs.KindValidationFailed, "managed Backing image requires a digest")
		}
		reference = dockerreference.TrimNamed(named).String() + "@" + pinned.Digest().String()
	}
	selected, err := service.imageRegistry.Resolve(ctx, reference)
	if err != nil {
		return creationImage{}, err
	}
	if selected.Requested != reference || selected.Validate() != nil {
		return creationImage{}, errs.New(errs.KindStateConflict, "Backing creation image selection changed")
	}
	seals, err := workloadseal.Resolve(ctx, service.images, agent.Record.ID,
		[]workloadseal.Selection{{Requested: &workloadseal.Requested{Reference: selected.Reference(), Replicas: 1}}})
	if err != nil {
		return creationImage{}, err
	}
	return creationImage{
		reference:    strings.SplitN(authored, "@", 2)[0] + "@" + selected.ManifestDigest,
		localID:      seals[0].LocalImageID,
		architecture: selected.Architecture,
		variant:      selected.Variant,
	}, nil
}
