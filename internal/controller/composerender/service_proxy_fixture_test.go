package composerender

import (
	strings "strings"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	domain "github.com/AlanD20/groundplane/internal/core/release"
)

func testServiceProxyImage() *domain.ProxyImage {
	return &domain.ProxyImage{Repository: "docker.io/library/caddy", IndexDigest: strings.Repeat("a", 64),
		Platform: componentsdk.OCIPlatform{OS: "linux", Architecture: "amd64",
			ChildDigest: strings.Repeat("b", 64), ConfigDigest: strings.Repeat("c", 64)}}
}

func uint32Pointer(value uint32) *uint32 {
	return &value
}
