//go:build linux

package entrymaterializer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// SVC-15/JOURNEY-02: independent recovery proof must reject changed bytes,
// metadata or presence even when the runtime itself appears healthy.
func TestVerifyPinnedFilePostconditionWithoutRepair(t *testing.T) {
	t.Parallel()
	content := []byte("pinned configuration")
	for _, scenario := range []string{"exact", "different bytes", "permissions", "owner", "missing", "symlink", "hardlink"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			materializer, root := testMaterializer(t)
			header := testHeader(t, "files/config", entrymaterialization.OutputSecretFile, content)
			if err := materializer.materialize(t.Context(), header, bytes.NewReader(content)); err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(root, "files/config")
			expectedKind := errs.KindStateConflict
			switch scenario {
			case "different bytes":
				if err := os.WriteFile(destination, bytes.Repeat([]byte("x"), len(content)), 0o600); err != nil {
					t.Fatal(err)
				}
			case "permissions":
				if err := os.Chmod(destination, 0o444); err != nil {
					t.Fatal(err)
				}
			case "owner":
				// The expected owner changes, not host ownership; this assertion
				// also runs without privilege to chown to another user.
				spec := testHeaderSpec("files/config", entrymaterialization.OutputSecretFile, content)
				spec.UID++
				header = mustHeader(t, spec)
			case "missing":
				if err := os.Remove(destination); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(destination, destination+"-original"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("config-original", destination); err != nil {
					t.Fatal(err)
				}
				expectedKind = errs.KindInternal
			case "hardlink":
				if err := os.Link(destination, destination+"-alias"); err != nil {
					t.Fatal(err)
				}
				expectedKind = errs.KindInternal
			}
			before, beforeErr := os.Lstat(destination)
			err := materializer.verify(t.Context(), header)
			if scenario == "exact" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, errs.New(expectedKind, "")) {
				t.Fatalf("verification error = %v, want %v", err, expectedKind)
			}
			after, afterErr := os.Lstat(destination)
			if (beforeErr == nil) != (afterErr == nil) || beforeErr == nil &&
				(!os.SameFile(before, after) || before.Mode() != after.Mode() || before.Size() != after.Size()) {
				t.Fatal("verification changed the selected file")
			}
			assertNoTemporary(t, filepath.Join(root, "files"))
		})
	}
}

// SVC-15: an independently read inode is not proof for a destination replaced
// during observation. Recovery must re-probe instead of acknowledging it.
func TestVerifyRejectsDestinationReplacementDuringObservation(t *testing.T) {
	t.Parallel()
	ops := productionLinuxOps()
	open := ops.openat2
	var replacement, destination string
	armed := false
	ops.openat2 = func(parent int, name string, how *unix.OpenHow) (int, error) {
		if armed && name == "config" && how.Flags&unix.O_PATH != 0 {
			armed = false
			if err := os.Rename(replacement, destination); err != nil {
				return -1, err
			}
		}
		return open(parent, name, how)
	}
	writer, root := testMaterializerWithOps(t, ops)
	content := []byte("pinned")
	header := testHeader(t, "files/config", entrymaterialization.OutputSecretFile, content)
	if err := writer.materialize(t.Context(), header, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	destination, replacement = filepath.Join(root, "files/config"), filepath.Join(root, "files/replacement")
	if err := os.WriteFile(replacement, content, 0o600); err != nil {
		t.Fatal(err)
	}
	armed = true
	if err := writer.verify(t.Context(), header); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("replaced destination was accepted: %v", err)
	}
}

// SVC-15: initial absence is authority, not permission to preserve or delete
// an unexpected host file during a read-only probe.
func TestVerifyPinnedAbsenceDoesNotCreateParentsOrRemoveFiles(t *testing.T) {
	t.Parallel()
	materializer, root := testMaterializer(t)
	header := testHeader(t, "files/config", entrymaterialization.OutputRemoveSecretFile, nil)
	if err := materializer.verify(t.Context(), header); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "files")); !os.IsNotExist(err) {
		t.Fatal("absence verification created a parent")
	}
	content := []byte("unexpected")
	write := testHeader(t, "files/config", entrymaterialization.OutputSecretFile, content)
	if err := materializer.materialize(t.Context(), write, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	if err := materializer.verify(t.Context(), header); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("existing file: %v", err)
	}
	actual, err := os.ReadFile(filepath.Join(root, "files/config"))
	if err != nil || !bytes.Equal(actual, content) {
		t.Fatal("absence verification removed or changed the file")
	}
}

// SVC-15: malformed delivery must not reach the filesystem. Valid verification
// consumes and closes its frame and independently opens the actual file.
func TestVerifyAuthenticatesFrameBeforeRootAccess(t *testing.T) {
	t.Parallel()
	content := []byte("original")
	header := testHeader(t, "files/config", entrymaterialization.OutputSecretFile, content)
	limits := testLimits(content)
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid", true: "truncated"}[corrupt], func(t *testing.T) {
			writer, _ := testMaterializer(t)
			if err := writer.materialize(t.Context(), header, bytes.NewReader(content)); err != nil {
				t.Fatal(err)
			}
			frame := testFrame(t, header, content, limits)
			if corrupt {
				frame = frame[:len(frame)-1]
			}
			opened := false
			err := verify(t.Context(), io.NopCloser(bytes.NewReader(frame)), limits,
				func(context.Context) (*materializer, error) { opened = true; return writer, nil })
			if corrupt {
				if err == nil || opened {
					t.Fatalf("malformed frame reached root: %v", err)
				}
			} else if err != nil || !opened {
				t.Fatalf("valid verification failed: %v", err)
			}
		})
	}
}
