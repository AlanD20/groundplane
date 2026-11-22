package consolerelease

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"testing/fstest"
	"time"
)

// Rationale: All upgrades stage a future Console while the predecessor API is
// live. The new Controller must keep its API online with Console gated, then
// activate the exact matching bundle and retain fingerprinted predecessor
// assets for already-open tabs.
func TestPairedAPIMigrationStagesBeforeBootAndActivatesAfterBoot(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "console")
	oldAPI := []byte(`{"openapi":"old"}`)
	newAPI := []byte(`{"openapi":"new"}`)
	bootstrap := fstest.MapFS{
		"index.html":             &fstest.MapFile{Data: []byte("old-index")},
		"assets/app-11111111.js": &fstest.MapFile{Data: []byte("old-js")},
	}
	oldStore, err := New(root, bootstrap, "console/v0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := oldStore.BindAPIDocument(oldAPI); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "console.tar.gz")
	bundleDigest := writeTestBundle(t, archive, "console/v0.0.2", APIHash(newAPI), map[string][]byte{
		"index.html":             []byte("new-index"),
		"assets/app-22222222.js": []byte("new-js"),
	})
	if _, err := oldStore.StageArchive(context.Background(), archive, "console/v0.0.2", bundleDigest,
		"1123456789abcdef0123456789abcdef01234567"); err == nil {
		t.Fatal("Console bundle with another source commit was accepted")
	}
	artifact, err := oldStore.StageArchive(context.Background(), archive, "console/v0.0.2", bundleDigest,
		"0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatalf("stage future Console under old API: %v", err)
	}
	if err := oldStore.Compatible(artifact); err == nil {
		t.Fatal("future Console unexpectedly compatible with old API")
	}

	newStore, err := New(root, bootstrap, "console/v0.0.2")
	if err != nil {
		t.Fatal(err)
	}
	if err := newStore.BindAPIDocument(newAPI); err != nil {
		t.Fatalf("new Controller API failed to boot with predecessor Console: %v", err)
	}
	if _, err := newStore.Assets(); err == nil {
		t.Fatal("mismatched predecessor Console was served")
	}
	if err := newStore.Activate(artifact); err != nil {
		t.Fatalf("activate matching Console: %v", err)
	}
	assets, err := newStore.Assets()
	if err != nil {
		t.Fatal(err)
	}
	assertTestAsset(t, assets, "index.html", "new-index")
	assertTestAsset(t, assets, "assets/app-11111111.js", "old-js")
}

func TestBindRecoversReceiptSyncedBeforeAtomicRename(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "console")
	api := []byte(`{"openapi":"same"}`)
	bootstrap := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("old")}}
	store, err := New(root, bootstrap, "console/v0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindAPIDocument(api); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "console.tar.gz")
	bundleDigest := writeTestBundle(t, archive, "console/v0.0.2", APIHash(api),
		map[string][]byte{"index.html": []byte("recovered")})
	artifact, err := store.StageArchive(context.Background(), archive, "console/v0.0.2", bundleDigest,
		"0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatal(err)
	}
	next := store.receipt
	previous := next.Active
	next.Active, next.Predecessor, next.ActivatedAt = artifact, &previous, time.Unix(2, 0).UTC()
	value, err := json.Marshal(next)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".active-next"), value, 0o600); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(root, bootstrap, "console/v0.0.2")
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.BindAPIDocument(api); err != nil {
		t.Fatalf("recover staged receipt: %v", err)
	}
	if current, found := restarted.Current(); !found || current != artifact {
		t.Fatalf("current = %#v, %v; want recovered %#v", current, found, artifact)
	}
	assets, err := restarted.Assets()
	if err != nil {
		t.Fatal(err)
	}
	assertTestAsset(t, assets, "index.html", "recovered")
}

func writeTestBundle(t *testing.T, destination, release, apiHash string, assets map[string][]byte) string {
	t.Helper()
	paths := make([]string, 0, len(assets))
	for name := range assets {
		paths = append(paths, name)
	}
	sort.Strings(paths)
	files := make([]File, 0, len(paths))
	for _, name := range paths {
		files = append(files, File{Path: name, Size: int64(len(assets[name])), SHA256: digest(assets[name])})
	}
	manifest, _, err := encodeManifest(Manifest{
		Schema: 1, Release: release, SourceCommit: "0123456789abcdef0123456789abcdef01234567",
		APISHA256: apiHash, CreatedAt: time.Unix(1, 0).UTC(), Files: files,
	})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	compressed := gzip.NewWriter(io.MultiWriter(file, hash))
	archive := tar.NewWriter(compressed)
	write := func(name string, value []byte) {
		t.Helper()
		header := &tar.Header{Name: name, Mode: 0o600, Size: int64(len(value)), Typeflag: tar.TypeReg}
		if err := archive.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := archive.Write(value); err != nil {
			t.Fatal(err)
		}
	}
	write(manifestName, manifest)
	for _, name := range paths {
		write(name, assets[name])
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := compressed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func assertTestAsset(t *testing.T, assets fs.FS, name, want string) {
	t.Helper()
	value, err := fs.ReadFile(assets, name)
	if err != nil || string(value) != want {
		t.Fatalf("asset %s = %q, %v; want %q", name, value, err, want)
	}
}
