package softwarepreparation

import (
	"os"
	"path/filepath"
	"testing"

	buildversion "github.com/AlanD20/groundplane/internal/common/version"
)

// The native release job supplies its produced archive and independent Git
// identity. Qualification must use the updater's reader before publication;
// agreeing packager/installer readers alone cannot establish update admission.
func TestPublishedControllerBundleAdmission(t *testing.T) {
	archive := os.Getenv("GROUNDPLANE_RELEASE_BUNDLE")
	version := os.Getenv("GROUNDPLANE_RELEASE_VERSION")
	commit := os.Getenv("GROUNDPLANE_RELEASE_COMMIT")
	if archive == "" && version == "" && commit == "" {
		t.Skip("run by the native release bundle job with its produced artifact")
	}
	if archive == "" || version == "" || commit == "" {
		t.Fatal("release admission requires archive, version and independent source identity")
	}
	digests, err := extractControllerBundle(
		archive, t.TempDir(), version, HostPlatform(),
		buildversion.BuildProvenance{Kind: "release", Commit: commit},
	)
	if err != nil {
		t.Fatalf("published Controller bundle cannot enter normal software preparation: %v", err)
	}
	if digests.binary == "" || digests.metadata == "" || digests.cli == "" {
		t.Fatal("accepted Controller bundle did not retain its verified payload identities")
	}
	api, err := os.ReadFile(filepath.Join("..", "..", "..", "openapi.json"))
	if err != nil {
		t.Fatal(err)
	}
	if want := sha256Value(api); digests.api != want {
		t.Fatalf("prepared Controller API digest = %q, want source API digest %q", digests.api, want)
	}
}
