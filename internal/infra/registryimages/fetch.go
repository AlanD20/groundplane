package registryimages

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/containerd/errdefs"
	"github.com/distribution/reference"
	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Fetch uses only pinned manifest content. It neither changes a Service nor
// moves a local mutable tag. It returns the verified OCI configuration digest,
// independent of the host Docker store's local identifier representation.
func (registryClient *Client) Fetch(ctx context.Context, plan imagefetch.Plan, report imagefetch.Reporter) (string, error) {
	if err := plan.Validate(); err != nil {
		return "", err
	}
	if plan.Architecture != registryClient.architecture {
		return "", errs.New(errs.KindStateConflict, "image fetch plan belongs to another host platform")
	}
	observed, err := registryClient.engine.ImageInspect(ctx, plan.Reference())
	if err == nil {
		if err := report(imagefetch.Progress{Phase: "verifying"}); err != nil {
			return "", err
		}
		return verifyImage(observed, plan)
	}
	if !errdefs.IsNotFound(err) {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	authentication := ""
	if strings.HasPrefix(plan.Repository, imagefetch.RegistryAuthority+"/") {
		authentication, err = registryClient.dockerAuthentication()
		if err != nil {
			return "", errs.Wrap(errs.KindInternal, err)
		}
	}
	progress := imagefetch.Progress{Phase: "downloading"}
	if err := report(progress); err != nil {
		return "", err
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
	layers := make(map[string][2]int64)
	lastReport := time.Now()
	for message, streamErr := range pull.JSONMessages(ctx) {
		if streamErr != nil {
			return "", errs.Wrap(errs.KindInternal, streamErr)
		}
		if message.Error != nil {
			return "", errors.Join(errs.New(errs.KindStateConflict, "Docker could not download the selected image; check registry access and host disk space, then retry"), errs.Wrap(errs.KindInternal, message.Error))
		}
		if message.Status != "Downloading" || message.Progress == nil || message.ID == "" {
			continue
		}
		current, total := message.Progress.Current, message.Progress.Total
		if current < 0 || total < current || total > 1<<50 {
			continue
		}
		if _, exists := layers[message.ID]; !exists && len(layers) >= 4096 {
			continue
		}
		layers[message.ID] = [2]int64{current, total}
		progress.DownloadedBytes, progress.TotalBytes = 0, 0
		for _, layer := range layers {
			progress.DownloadedBytes += layer[0]
			progress.TotalBytes += layer[1]
		}
		if time.Since(lastReport) >= time.Second {
			if err := report(progress); err != nil {
				return "", err
			}
			lastReport = time.Now()
		}
	}
	progress.Phase = "verifying"
	if err := report(progress); err != nil {
		return "", err
	}
	observed, err = registryClient.engine.ImageInspect(ctx, plan.Reference())
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	return verifyImage(observed, plan)
}

func verifyImage(observed client.ImageInspectResult, plan imagefetch.Plan) (string, error) {
	if err := managedimage.Verify(observed.ID, observed.Descriptor, plan.ManifestDigest, plan.ConfigDigest,
		ocispec.Platform{OS: "linux", Architecture: plan.Architecture, Variant: plan.Variant}); err != nil {
		return "", err
	}
	if observed.Os != "linux" || observed.Architecture != plan.Architecture ||
		observed.Variant != plan.Variant || !hasRepositoryDigest(observed.RepoDigests, plan.Reference()) {
		return "", errs.New(errs.KindStateConflict, "host image differs from the pinned registry content")
	}
	return plan.ConfigDigest, nil
}

func hasRepositoryDigest(values []string, selected string) bool {
	for _, value := range values {
		named, err := reference.ParseNormalizedNamed(value)
		if err == nil && named.String() == selected {
			return true
		}
	}
	return false
}
