package registryimages

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

// List observes the same local daemon used by the single-host Agent. It does
// not read the registry catalog or infer availability from desired Service text.
func (Local) List(ctx context.Context) ([]imagefetch.LocalImage, error) {
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	images, listErr := engine.ImageList(ctx, client.ImageListOptions{Manifests: true})
	containers, containerErr := engine.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err := errors.Join(listErr, containerErr, engine.Close()); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	uses := make(map[string]int)
	for _, container := range containers.Items {
		uses[container.ImageID]++
	}
	result := make([]imagefetch.LocalImage, 0, len(images.Items))
	for _, image := range images.Items {
		contentIDs := []string{image.ID}
		blocked := ""
		if image.Descriptor != nil && strings.Contains(image.Descriptor.MediaType, "index") &&
			len(image.Manifests) == 0 {
			blocked = "Docker did not report this index's child image identities"
		}
		for _, manifest := range image.Manifests {
			contentIDs = append(contentIDs, manifest.ID)
		}
		containers := 0
		for _, id := range contentIDs {
			containers += uses[id]
		}
		result = append(result, imagefetch.LocalImage{
			ID: image.ID, Tags: imageReferences(image.RepoTags), Digests: imageReferences(image.RepoDigests),
			SizeBytes: image.Size, Created: image.Created, Containers: containers, ContentIDs: contentIDs, RemovalBlocked: blocked,
		})
	}
	slices.SortFunc(result, func(a, b imagefetch.LocalImage) int { return strings.Compare(a.ID, b.ID) })
	return result, nil
}

func (Local) Remove(ctx context.Context, id string) error {
	if !workloadimage.LocalIDValid(id) {
		return errs.New(errs.KindValidationFailed, "image removal requires a full local image ID")
	}
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	// Never force, untag by mutable name, or prune unrelated children.
	_, removeErr := engine.ImageRemove(ctx, id, client.ImageRemoveOptions{Force: false, PruneChildren: false})
	closeErr := engine.Close()
	if errdefs.IsNotFound(removeErr) {
		removeErr = nil
	}
	if errdefs.IsConflict(removeErr) && closeErr == nil {
		return errs.New(
			errs.KindResourceInUse,
			"Docker protects this image because it has containers, dependent images or multiple references",
		)
	}
	if err := errors.Join(removeErr, closeErr); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func imageReferences(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" && !strings.Contains(value, "<none>") {
			result = append(result, value)
		}
	}
	slices.Sort(result)
	return slices.Compact(result)
}
