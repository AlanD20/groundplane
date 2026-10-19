package app

import (
	"strings"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
)

func blueprintTestProxyCatalog(child string) componentsdk.OCIImage {
	return componentsdk.OCIImage{Repository: "docker.io/library/caddy", IndexDigest: strings.Repeat("a", 64),
		Platforms: []componentsdk.OCIPlatform{
			{
				OS:           "linux",
				Architecture: "amd64",
				ChildDigest:  strings.Repeat(child, 64),
				ConfigDigest: strings.Repeat("c", 64),
			},
			{
				OS:           "linux",
				Architecture: "arm64",
				Variant:      "v8",
				ChildDigest:  strings.Repeat(child+"1", 32),
				ConfigDigest: strings.Repeat("f", 64),
			},
		}}
}
