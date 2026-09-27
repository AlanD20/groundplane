package registryimages

import (
	"context"
	"errors"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Fetch uses only pinned manifest content. It neither changes a Service nor
// moves a local mutable tag, so callers can safely Deploy the returned reference.
func (registryClient *Client) Fetch(ctx context.Context, plan imagefetch.Plan) (string, error) {
	if err := plan.Validate(); err != nil {
		return "", err
	}
	if plan.Architecture != registryClient.architecture {
		return "", errs.New(errs.KindStateConflict, "image fetch plan belongs to another host platform")
	}
	observed, err := registryClient.engine.ImageInspect(ctx, plan.Reference())
	if err == nil {
		return verifyImage(observed, plan)
	}
	if !errdefs.IsNotFound(err) {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	authentication, err := registryClient.dockerAuthentication()
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	pull, err := registryClient.engine.ImagePull(ctx, plan.Reference(), client.ImagePullOptions{
		RegistryAuth: authentication,
		Platforms:    []ocispec.Platform{{OS: "linux", Architecture: plan.Architecture, Variant: plan.Variant}},
	})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	if pull == nil {
		return "", errs.New(errs.KindInternal, "image fetch returned no pull result")
	}
	waitErr := pull.Wait(ctx)
	closeErr := pull.Close()
	if err := errors.Join(waitErr, closeErr); err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	observed, err = registryClient.engine.ImageInspect(ctx, plan.Reference())
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	return verifyImage(observed, plan)
}

func verifyImage(observed client.ImageInspectResult, plan imagefetch.Plan) (string, error) {
	if observed.ID != plan.ConfigDigest || observed.Os != "linux" || observed.Architecture != plan.Architecture ||
		observed.Variant != plan.Variant || !slices.Contains(observed.RepoDigests, plan.Reference()) {
		return "", errs.New(errs.KindStateConflict, "host image differs from the pinned registry content")
	}
	return observed.ID, nil
}
