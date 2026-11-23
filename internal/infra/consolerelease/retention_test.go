package consolerelease

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestConsoleArchiveCapacityReclaimsOnlyExpiredRetiredBundles(t *testing.T) {
	t.Parallel()
	api := []byte(`{"openapi":"3.1.0"}`)
	root := filepath.Join(t.TempDir(), "console")
	bootstrap := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte(strings.Repeat("x", 8<<10))},
	}
	store, err := New(root, bootstrap, "console/v0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(10, 0).UTC()
	store.now = func() time.Time { return now }
	if err := store.BindAPIDocument(api); err != nil {
		t.Fatal(err)
	}
	expiring, _ := store.Current()
	stageAndActivate := func(release, asset string) Artifact {
		t.Helper()
		bundle, err := store.stageFS(fstest.MapFS{
			"index.html": &fstest.MapFile{Data: []byte(release)},
			asset:        &fstest.MapFile{Data: []byte(release)},
		}, release, strings.Repeat("a", 40), APIHash(api))
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Activate(bundle); err != nil {
			t.Fatal(err)
		}
		return bundle
	}
	reused := stageAndActivate("console/v0.0.2", "assets/reused-11111111.js")
	previous := stageAndActivate("console/v0.0.3", "assets/previous-22222222.js")
	active := stageAndActivate("console/v0.0.4", "assets/active-33333333.js")
	const ceiling = 6 << 10
	if kind, _ := errs.KindOf(store.admitArchive(reused.ManifestSHA256, 2048, ceiling)); kind != errs.KindResourceInUse {
		t.Fatalf("archive admission must protect unexpired assets, got error kind %v", kind)
	}
	now = now.Add(retiredAssetWindow)
	if err := store.admitArchive(reused.ManifestSHA256, 2048, ceiling); err != nil {
		t.Fatalf("expired bundle left archived release admission blocked: %v", err)
	}
	if _, err := os.Stat(store.releasePath(expiring.ManifestSHA256)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expired bundle was not reclaimed: %v", err)
	}
	if err := store.Verify(reused); err != nil {
		t.Fatalf("re-prepared bundle was removed: %v", err)
	}
	reopened, err := New(root, bootstrap, "console/v0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.BindAPIDocument(api); err != nil {
		t.Fatal(err)
	}
	assets, err := reopened.Assets()
	if err != nil {
		t.Fatal(err)
	}
	assertTestAsset(t, assets, "index.html", active.Release)
	assertTestAsset(t, assets, "assets/previous-22222222.js", previous.Release)
}
