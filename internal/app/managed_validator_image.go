package app

import (
	"runtime"

	"github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelpercontainer"
)

func selectedValidatorImage(image component.OCIImage) managedconfighelpercontainer.ValidatorImage {
	platform, reference, _ := image.Select(runtime.GOOS, runtime.GOARCH)
	return managedconfighelpercontainer.ValidatorImage{Reference: reference, Platform: platform}
}
