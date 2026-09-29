package imagedelivery

import (
	"context"
	"fmt"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func (service *Service) ListImages(ctx context.Context) (apiTypes.ImageList, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	images, err := service.registry.List(ctx)
	if err != nil {
		return apiTypes.ImageList{}, err
	}
	retention, err := service.retainedImages(ctx, "")
	if err != nil {
		return apiTypes.ImageList{}, err
	}
	fence, _, err := readFence(ctx, service.store)
	if err != nil {
		return apiTypes.ImageList{}, err
	}
	if fence.Operation != "" {
		retention.busy = "An image removal must finish or be retried first"
	}
	result := apiTypes.ImageList{
		Images:     make([]apiTypes.HostImage, 0, len(images)),
		ObservedAt: time.Now().UTC().Format(time.RFC3339),
	}
	owners := make(map[string]*apiTypes.ImageContainerOwner)
	for _, image := range images {
		uses, err := service.containerUses(ctx, image.ContainerUses, owners)
		if err != nil {
			return apiTypes.ImageList{}, err
		}
		result.Images = append(result.Images, apiTypes.HostImage{
			ID: image.ID, Tags: image.Tags, Digests: image.Digests, SizeBytes: image.SizeBytes,
			CreatedAt: time.Unix(image.Created, 0).UTC().Format(time.RFC3339), Containers: image.Containers,
			RemovalBlocked: removalReason(image, retention),
			ContainerUses:  uses, ProtectionReason: otherRemovalReason(image, retention),
			Fetches: retention.history(image),
		})
	}
	return result, nil
}

func removalReason(image imagefetch.LocalImage, retention imageRetention) string {
	if image.Containers > 0 {
		return fmt.Sprintf("Used by %d containers, including stopped containers", image.Containers)
	}
	return otherRemovalReason(image, retention)
}

func otherRemovalReason(image imagefetch.LocalImage, retention imageRetention) string {
	if image.RemovalBlocked != "" {
		return image.RemovalBlocked
	}
	for _, value := range append(append([]string{image.ID}, image.ContentIDs...), image.Digests...) {
		if reason := retention.references[canonicalImageReference(value)]; reason != "" {
			return reason
		}
	}
	return retention.busy
}
