package taskplanning

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
				ConfigDigest: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			},
			{
				OS:           "linux",
				Architecture: "arm64",
				Variant:      "v8",
				ChildDigest:  "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				ConfigDigest: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			},
		},
	}
}

func controllerTestOCIPlatform(repository string) (componentsdk.OCIImage, componentsdk.OCIPlatform, string) {
	image := controllerTestOCIImage(repository)
	platform, reference, selected := image.Select(runtime.GOOS, runtime.GOARCH)
	if !selected {
		panic("controller test OCI image does not support the current platform")
	}
	return image, platform, reference
}

func controllerTestOCIImageReference(repository string) string {
	_, _, reference := controllerTestOCIPlatform(repository)
	return reference
}
