package controllerrelease

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// Rationale: a private leaf does not prevent replacement of the whole release
// directory by a writer of its parent. Every ancestor must be trusted too.
func TestReleaseStoreRejectsWritableAncestor(t *testing.T) {
	parent := t.TempDir()
	leaf := filepath.Join(parent, "private")
	if err := os.Mkdir(leaf, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o770); err != nil {
		t.Fatal(err)
	}
	store, err := openStore(context.Background(), leaf, uint32(os.Geteuid()))
	if store != nil {
		defer store.Close()
	}
	if err == nil {
		t.Fatal("private release leaf below a group-writable parent was accepted")
	}
}
