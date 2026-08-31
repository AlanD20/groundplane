package controller

import (
	"runtime"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
)

func controllerTestOCIImage(repository string) componentsdk.OCIImage {
	return componentsdk.OCIImage{
		Repository:  repository,
		IndexDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Platforms: []componentsdk.OCIPlatform{
			{
				OS:           "linux",
				Architecture: "amd64",
				ChildDigest:  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			},
			{
				OS:           "linux",
				Architecture: "arm64",
				Variant:      "v8",
				ChildDigest:  "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			},
		},
	}
}

func controllerTestOCIPlatform(repository string) (componentsdk.OCIImage, componentsdk.OCIPlatform, string) {
	image := controllerTestOCIImage(repository)
	variant := ""
	if runtime.GOARCH == "arm64" {
		variant = "v8"
	}
	platform, reference, selected := image.Select(runtime.GOOS, runtime.GOARCH, variant)
	if !selected {
		panic("controller test OCI image does not support the current platform")
	}
	return image, platform, reference
}

func controllerTestOCIImageReference(repository string) string {
	_, _, reference := controllerTestOCIPlatform(repository)
	return reference
}
