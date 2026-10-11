package blueprintrelease

import component "github.com/AlanD20/groundplane-component-sdk/component"

// Shared immutable OCI fixtures used by Blueprint Service-unit preparation.
const (
	testManagedServiceImageRepository     = "docker.io/cloudflare/cloudflared"
	testManagedServiceImageIndexDigest    = "4f6655284ab3d252b7f28fedb19fe6c8fc82ee5b1295c20ac74d475e5398a52d"
	testManagedServiceAMD64ManifestDigest = "18626b1baac4450214535cd5bc40ef44c0635244d585ebf707749c22b6f3408f"
	testManagedServiceAMD64ConfigDigest   = "e871921d7924ab4baa36da9938ecddb86025b5b1aa930500769456bb24f50a75"
	testManagedServiceARM64ManifestDigest = "a85d5a3d6f22cb3c7e78b2f0d05b0f0daeb72566e9426f656c60b357b7b89c95"
	testManagedServiceARM64ConfigDigest   = "5d249c08c07ddc00eb501917e030874347a8ea786bdb644bb2cff92a4cd2b843"
	testRouterServiceImageRepository      = "docker.io/library/caddy"
	testRouterServiceImageIndexDigest     = "5f5c8640aae01df9654968d946d8f1a56c497f1dd5c5cda4cf95ab7c14d58648"
	testRouterServiceAMD64ManifestDigest  = "98eb57d882ccd5213d1688764db10c1ca2c58a1ca3a6717a3411ad798f7a423a"
	testRouterServiceAMD64ConfigDigest    = "af555904a0961945f16bb323a501457b13a4f7e9bde969b145b97da80b38ecbe"
	testRouterServiceARM64ManifestDigest  = "1172d4213087d3fc30bafc7ff2c2896180eb0c41ff7f75f315568fb36cabdcba"
	testRouterServiceARM64ConfigDigest    = "6b08c1b9858ca9a7d99c1da13c3695081e0e604c6cf214ca26a7ce0e2c4fd9b4"
)

func managedServiceTestImage() component.OCIImage {
	return component.OCIImage{
		Repository:  testManagedServiceImageRepository,
		IndexDigest: testManagedServiceImageIndexDigest,
		Platforms: []component.OCIPlatform{
			{
				OS: "linux", Architecture: "amd64",
				ChildDigest:  testManagedServiceAMD64ManifestDigest,
				ConfigDigest: testManagedServiceAMD64ConfigDigest,
			},
			{
				OS: "linux", Architecture: "arm64",
				ChildDigest:  testManagedServiceARM64ManifestDigest,
				ConfigDigest: testManagedServiceARM64ConfigDigest,
			},
		},
	}
}

func routerServiceTestImage() component.OCIImage {
	return component.OCIImage{
		Repository:  testRouterServiceImageRepository,
		IndexDigest: testRouterServiceImageIndexDigest,
		Platforms: []component.OCIPlatform{
			{
				OS: "linux", Architecture: "amd64",
				ChildDigest:  testRouterServiceAMD64ManifestDigest,
				ConfigDigest: testRouterServiceAMD64ConfigDigest,
			},
			{
				OS: "linux", Architecture: "arm64", Variant: "v8",
				ChildDigest:  testRouterServiceARM64ManifestDigest,
				ConfigDigest: testRouterServiceARM64ConfigDigest,
			},
		},
	}
}
