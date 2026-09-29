package registryimages

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/pkg/errs"
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
	uses := make(map[string][]imagefetch.ContainerUse)
	for _, container := range containers.Items {
		name := container.ID
		if len(container.Names) > 0 {
			name = strings.TrimPrefix(container.Names[0], "/")
		}
		uses[container.ImageID] = append(uses[container.ImageID], imagefetch.ContainerUse{
			ID: container.ID, Name: name, State: string(container.State),
			Managed:       container.Labels["com.groundplane.managed"] == "true",
			EnvironmentID: container.Labels["com.groundplane.environment-id"],
			ServiceID:     container.Labels["com.groundplane.service-id"],
		})
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
		slices.Sort(contentIDs)
		contentIDs = slices.Compact(contentIDs)
		containerUses := make([]imagefetch.ContainerUse, 0)
		for _, id := range contentIDs {
			containerUses = append(containerUses, uses[id]...)
		}
		slices.SortFunc(
			containerUses,
			func(a, b imagefetch.ContainerUse) int { return strings.Compare(a.Name, b.Name) },
		)
		tags, digests := []string{}, slices.Clone(image.RepoDigests)
		for _, ref := range image.RepoTags {
			if strings.Contains(ref, "@") {
				digests = append(digests, ref)
			} else {
				tags = append(tags, ref)
			}
		}
		result = append(result, imagefetch.LocalImage{
			ID: image.ID, Tags: imageReferences(tags), Digests: imageReferences(digests),
			SizeBytes: image.Size, Created: image.Created, Containers: len(containerUses), ContainerUses: containerUses,
			ContentIDs: contentIDs, RemovalBlocked: blocked,
		})
	}
	slices.SortFunc(result, func(a, b imagefetch.LocalImage) int { return strings.Compare(a.ID, b.ID) })
	return result, nil
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
