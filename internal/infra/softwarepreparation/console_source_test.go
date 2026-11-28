package softwarepreparation

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/consolerelease"
)

// Source preparation must retain the active Console until Apply. The paired
// Console can activate only after the new Controller provides its exact API.
func TestSourceConsoleStagedForIncomingAPI(t *testing.T) {
	ctx := context.Background()
	workspace := t.TempDir()
	dist := filepath.Join(workspace, "controller-payload", "console-dist")
	if err := os.MkdirAll(dist, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dist, "index.html"), []byte("source-built index"), 0o600); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(workspace, "store")
	bootstrap := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("previous index")}}
	store, err := consolerelease.New(root, bootstrap, "console/v0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindAPIDocument([]byte("previous API")); err != nil {
		t.Fatal(err)
	}
	preparer := &Preparer{console: store}
	source := ResolvedSource{Selection: SelectionAll, SourceKind: SourceRef, ResolvedSHA: strings.Repeat("a", 40),
		CommitTime: time.Unix(1784561234, 0).UTC(), OriginalRef: "main", Platform: HostPlatform()}
	api := []byte("incoming API")
	artifact, err := preparer.prepareConsoleSource(
		ctx,
		source,
		controllerDigests{api: consolerelease.APIHash(api)},
		workspace,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.VerifyActivation(artifact); err != nil {
		t.Fatal(err)
	}
	if err := store.Activate(artifact); err == nil {
		t.Fatal("predecessor activated the incompatible Console")
	}
	if current, ok := store.Current(); !ok || current.Release != "console/v0.0.1" {
		t.Fatalf("preparation changed the active Console: %v", current.Release)
	}
	successor, err := consolerelease.New(root, bootstrap, "console/v0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := successor.BindAPIDocument(api); err != nil {
		t.Fatal(err)
	}
	if err := successor.Activate(artifact); err != nil {
		t.Fatal(err)
	}
	assets, err := successor.Assets()
	if err != nil {
		t.Fatal(err)
	}
	index, err := fs.ReadFile(assets, "index.html")
	if err != nil || string(index) != "source-built index" {
		t.Fatalf("active index = %q, err = %v", index, err)
	}
	version, err := fs.ReadFile(assets, "console-version.json")
	if err != nil || string(version) != `{"release":"console/ref/`+source.ResolvedSHA+`"}` {
		t.Fatalf("active version = %q, err = %v", version, err)
	}
}
