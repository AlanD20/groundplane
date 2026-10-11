package envoy

import "github.com/AlanD20/groundplane-component-sdk/component"

// Image is upstream Envoy v1.39.3, not a Groundplane-specific wrapper. The
// index, serving children and configuration digests were captured together.
var Image = component.OCIImage{
	Repository: "docker.io/envoyproxy/envoy", Tag: "v1.39.3",
	IndexDigest: "dd85940439de19a0b6ae8419610363ea0ad351d9a994ea007161c206ec1e1865",
	Platforms: []component.OCIPlatform{
		{OS: "linux", Architecture: "amd64",
			ChildDigest:  "e877a955cc8fde1d54b6b4141b95240511fe1494a7d98759c71a0cf43573ae33",
			ConfigDigest: "8f34a3f1f8c804af3f975bc19a50104120482535401d5c8e954156c20ffd5316"},
		{OS: "linux", Architecture: "arm64",
			ChildDigest:  "fe2975b65b1412b537cbf0bee291d029e75af6eedf8122fac8576399f7f0a456",
			ConfigDigest: "f1f1883f08256bd0a7ad15f0d874ca1f39773409c960b55791dfbb5213a743a6"},
	},
}
