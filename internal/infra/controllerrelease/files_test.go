package controllerrelease

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// UP-03: an unstaged operator input is not a server failure. Missing store
// structure or files in an existing release must not be mistaken for that input.
func TestInspectMissingReleaseClassification(t *testing.T) {
	for _, mode := range []string{"absent-release", "absent-parent", "absent-manifest", "absent-binary", "closed-store"} {
		t.Run(mode, func(t *testing.T) {
			store, id, _ := testReleaseStore(t)
			defer store.Close()
			parent := filepath.Join(store.root.Name(), "releases")
			leaf := filepath.Join(parent, string(id)[7:])
			want := errs.KindInternal
			switch mode {
			case "absent-release":
				id = upgrade.Hash([]byte("not staged"))
				want = errs.KindValidationFailed
			case "absent-parent":
				if err := os.Rename(parent, parent+".retained"); err != nil {
					t.Fatal(err)
				}
			case "absent-manifest", "absent-binary":
				name := "manifest.json"
				if mode == "absent-binary" {
					name = "controller"
				}
				if err := os.Rename(filepath.Join(leaf, name), filepath.Join(leaf, name+".retained")); err != nil {
					t.Fatal(err)
				}
			case "closed-store":
				store.Close()
			}
			_, err := store.Inspect(context.Background(), id)
			if kind, ok := errs.KindOf(err); !ok || kind != want {
				t.Fatalf("Inspect error = %v; want kind %v", err, want)
			}
		})
	}
}

// Rationale: staging is a privileged filesystem boundary; its digest check
// must cover the bytes actually opened and reject symlinks and writable inputs.
func TestReleaseStageBoundary(t *testing.T) {
	for _, mode := range []string{"valid", "changed-binary", "symlink", "writable", "directory-symlink"} {
		t.Run(mode, func(t *testing.T) {
			store, id, binary := testReleaseStore(t)
			defer store.Close()
			leaf := filepath.Join(store.root.Name(), "releases", string(id)[7:])
			path := filepath.Join(leaf, "controller")
			switch mode {
			case "changed-binary":
				if err := os.Chmod(path, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("changed"), 0o500); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, 0o500); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(path, path+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("controller.original", path); err != nil {
					t.Fatal(err)
				}
			case "writable":
				if err := os.Chmod(path, 0o520); err != nil {
					t.Fatal(err)
				}
			case "directory-symlink":
				if err := os.Rename(leaf, leaf+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Base(leaf)+".original", leaf); err != nil {
					t.Fatal(err)
				}
			}
			manifest, err := store.Inspect(context.Background(), id)
			if mode == "valid" {
				if err != nil || manifest.ControllerSHA256 != upgrade.Hash(binary) {
					t.Fatalf("inspect = %#v, %v", manifest, err)
				}
			} else if err == nil {
				t.Fatal("unsafe release accepted")
			}
		})
	}
}

// Rationale: the predecessor is retained before mutation and every installed
// byte is rehashed. Failed digest validation must leave the existing target.
func TestVerifiedAtomicExecutableCopy(t *testing.T) {
	store, _, binary := testReleaseStore(t)
	defer store.Close()
	ctx := context.Background()
	if err := store.copyExecutable(ctx, store.root, "current", store.root, "previous", upgrade.Hash(binary)); err != nil {
		t.Fatal(err)
	}
	got, err := store.root.ReadFile("previous")
	if err != nil || string(got) != string(binary) {
		t.Fatalf("predecessor = %q, %v", got, err)
	}
	if err := store.copyExecutable(ctx, store.root, "current", store.root, "previous", upgrade.Hash([]byte("wrong"))); err == nil {
		t.Fatal("wrong digest accepted")
	}
	got, err = store.root.ReadFile("previous")
	if err != nil || string(got) != string(binary) {
		t.Fatalf("failed copy changed predecessor: %q, %v", got, err)
	}
}

func testReleaseStore(t *testing.T) (*Store, upgrade.Digest, []byte) {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := openStore(context.Background(), directory, uint32(os.Geteuid()))
	if err != nil {
		t.Fatal(err)
	}
	binary := []byte("verified executable bytes")
	manifest := []byte(`{"agent_image":"registry.example/agent@sha256:` + strings.Repeat("a", 64) +
		`","channel_schema":1,"controller_sha256":"` + string(upgrade.Hash(binary)) +
		`","controller_version":"0.1.0","schema":1,"storage_epoch":1}`)
	id := upgrade.Hash(manifest)
	leaf := filepath.Join(directory, "releases", string(id)[7:])
	if err := os.MkdirAll(leaf, 0o700); err != nil {
		t.Fatal(err)
	}
	for path, data := range map[string][]byte{filepath.Join(leaf, "manifest.json"): manifest,
		filepath.Join(leaf, "controller"): binary, filepath.Join(directory, "current"): binary} {
		if err := os.WriteFile(path, data, 0o500); err != nil {
			t.Fatal(err)
		}
	}
	return store, id, binary
}
