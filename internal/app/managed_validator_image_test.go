package app

import (
	"runtime"
	"testing"

	registeredcoredns "github.com/AlanD20/groundplane-registered-components/coredns"
)

// Rationale: config and child-manifest digests are different authorities;
// validator creation must receive the whole selected platform authority.
func TestSelectedValidatorImageKeepsConfigSeparateFromOCIReference(t *testing.T) {
	image := registeredcoredns.Image
	platform, reference, found := image.Select(runtime.GOOS, runtime.GOARCH)
	if !found {
		t.Fatal("test host not supported by registered image")
	}
	selected := selectedValidatorImage(image)
	if selected.Reference != reference || selected.Platform != platform {
		t.Fatalf("selected image = %+v, wanted reference %s and config %s", selected, reference, platform.ConfigDigest)
	}
	if selected.Platform.ConfigDigest == platform.ChildDigest || selected.Platform.ConfigDigest == image.IndexDigest {
		t.Fatal("config conflated with registry image digest")
	}
}
