package consoleplatform

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/version"
)

func TestInstalledVersionUsesSourceCommitForSourceBuild(t *testing.T) {
	t.Parallel()
	commit := "0123456789abcdef0123456789abcdef01234567"
	release, installed := installedVersion("0.0.1-ref."+commit, version.BuildProvenance{
		Kind: "source_ref", Commit: commit,
	})
	if !installed || release != "console/ref/"+commit {
		t.Fatalf("installedVersion = %q, %v", release, installed)
	}
	if release, installed := installedVersion("dev", version.BuildProvenance{}); installed || release != "" {
		t.Fatalf("development installedVersion = %q, %v", release, installed)
	}
}
