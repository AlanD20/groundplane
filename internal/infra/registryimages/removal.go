package registryimages

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/containerd/errdefs"
	"github.com/distribution/reference"
	"github.com/moby/moby/client"
)

func (Local) Remove(ctx context.Context, id string) error {
	if !workloadimage.LocalIDValid(id) {
		return errs.New(errs.KindValidationFailed, "image removal requires a full local image ID")
	}
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	removeErr := removeLocalImage(ctx, engine, id)
	if closeErr := engine.Close(); closeErr != nil {
		return errors.Join(removeErr, errs.Wrap(errs.KindInternal, closeErr))
	}
	return removeErr
}

// The Controller holds the durable image-selection fence and checks retained
// runtime authority. Repository selectors here remain content-addressed: never
// delete by a mutable tag, force removal, or prune unrelated image children.
func removeLocalImage(ctx context.Context, engine *client.Client, id string) error {
	observed, err := engine.ImageInspect(ctx, id, client.ImageInspectWithManifests(true))
	if errdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if observed.ID != id {
		return errs.New(errs.KindStateConflict, "Docker returned a different image identity")
	}
	selectors, err := removalSelectors(observed)
	if err != nil {
		return err
	}
	for _, selector := range selectors {
		current, err := engine.ImageInspect(ctx, selector)
		if errdefs.IsNotFound(err) {
			continue // This repository no longer references the selected content.
		}
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if current.ID != id {
			return errs.New(errs.KindStateConflict, "image reference changed before removal")
		}
		if err := checkRemovalContainers(ctx, engine, observed); err != nil {
			return err
		}
		if err := removeDockerReference(ctx, engine, selector); err != nil {
			return err
		}
	}
	// Untagging can succeed without deleting content. This exact-ID deletion is
	// mandatory: a partial untag is not a successful image removal.
	return removeDockerReference(ctx, engine, id)
}

func removalSelectors(observed client.ImageInspectResult) ([]string, error) {
	refs := imageReferences(append(slices.Clone(observed.RepoTags), observed.RepoDigests...))
	if len(refs) == 0 {
		return nil, nil
	}
	// Docker's descriptor binds the repository selector to the selected image,
	// including OCI indexes. A config ID cannot be used as a manifest digest.
	if observed.Descriptor == nil || observed.Descriptor.Digest.String() != observed.ID {
		return nil, nil // Exact-ID removal still works when no alias conflict exists.
	}
	selectors := make([]string, 0, len(refs))
	for _, value := range refs {
		named, err := reference.ParseNormalizedNamed(value)
		if err != nil {
			return nil, errs.New(errs.KindStateConflict, "Docker returned an invalid image reference")
		}
		pinned, err := reference.WithDigest(reference.TrimNamed(named), observed.Descriptor.Digest)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		selectors = append(selectors, pinned.String())
	}
	slices.Sort(selectors)
	return slices.Compact(selectors), nil
}

func checkRemovalContainers(ctx context.Context, engine *client.Client, image client.ImageInspectResult) error {
	ids := []string{image.ID}
	if image.Descriptor != nil && strings.Contains(image.Descriptor.MediaType, "index") && len(image.Manifests) == 0 {
		return errs.New(errs.KindResourceInUse, "Docker did not report the image index's child identities")
	}
	for _, manifest := range image.Manifests {
		ids = append(ids, manifest.ID)
	}
	containers, err := engine.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	for _, container := range containers.Items {
		if slices.Contains(ids, container.ImageID) {
			return errs.New(errs.KindResourceInUse, "image is used by a container, including stopped containers")
		}
	}
	return nil
}

func removeDockerReference(ctx context.Context, engine *client.Client, selector string) error {
	_, err := engine.ImageRemove(ctx, selector, client.ImageRemoveOptions{Force: false, PruneChildren: false})
	if errdefs.IsNotFound(err) {
		return nil
	}
	if errdefs.IsConflict(err) {
		return errs.New(errs.KindResourceInUse, "Docker still needs this image; container, dependency or reference protection prevents removal")
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
