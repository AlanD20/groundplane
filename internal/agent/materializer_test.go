package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	testTenantID      = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testProjectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testVolumeDir     = "/infra/vol/" + testTenantID + "/" + testProjectID + "/" + testEnvironmentID
)

// Rationale: a successful write must preserve Controller-rendered bytes while
// publishing only a private file and clearing the transferred plaintext.
func TestFileMaterializerWritesExactPrivateFileAndClearsPlaintext(t *testing.T) {
	t.Parallel()

	hostRoot := t.TempDir()
	materializer := testFileMaterializer(hostRoot)
	content := []byte("A=\"one\"\nTOKEN=\"$$value\"\n")
	want := append([]byte(nil), content...)

	err := materializer.materialize(
		context.Background(),
		testVolumeDir,
		"secrets/.env.env_01",
		content,
	)
	if err != nil {
		t.Fatalf("materialize() error = %v", err)
	}
	path := materializedTestPath(hostRoot, "secrets/.env.env_01")
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read materialized file: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("materialized bytes = %q, want %q", got, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat materialized file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("materialized mode = %04o, want 0600", info.Mode().Perm())
	}
	if !allBytesZero(content) {
		t.Fatal("materialize() retained caller-owned plaintext")
	}
}

// Rationale: publishing a root-owned file secret would guess a workload
// identity, so the public operation must remain closed until ADR 0020 lands.
func TestMaterializeFileSecretFailsClosedUntilOwnershipContractIsAccepted(t *testing.T) {
	t.Parallel()

	content := []byte("file-secret")
	err := MaterializeFileSecret(
		context.Background(),
		testVolumeDir,
		"config/identity.key",
		content,
	)
	if !errors.Is(err, errs.New(errs.KindNotImplemented, "")) {
		t.Fatalf("MaterializeFileSecret() error = %v, want not implemented", err)
	}
	if !allBytesZero(content) {
		t.Fatal("MaterializeFileSecret() retained plaintext")
	}
}

// Rationale: ambiguous or traversing destinations must be rejected before the
// Agent creates any host path, while their plaintext is still cleared.
func TestFileMaterializerRejectsNonCanonicalPathsBeforeMutation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		volumeDir string
		relPath   string
	}{
		{
			name:      "relative volume",
			volumeDir: strings.TrimPrefix(testVolumeDir, "/"),
			relPath:   "secret",
		},
		{name: "host root", volumeDir: "/", relPath: "secret"},
		{name: "unclean volume", volumeDir: testVolumeDir + "/..", relPath: "secret"},
		{name: "absolute destination", volumeDir: testVolumeDir, relPath: "/outside"},
		{name: "parent traversal", volumeDir: testVolumeDir, relPath: "../outside"},
		{name: "embedded traversal", volumeDir: testVolumeDir, relPath: "config/../outside"},
		{name: "directory destination", volumeDir: testVolumeDir, relPath: "."},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			hostRoot := t.TempDir()
			materializer := testFileMaterializer(hostRoot)
			content := []byte("do-not-retain")
			err := materializer.materialize(
				context.Background(), test.volumeDir, test.relPath, content,
			)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("materialize() error = %v, want validation failure", err)
			}
			if !allBytesZero(content) {
				t.Fatal("rejected plaintext was not cleared")
			}
			entries, readErr := os.ReadDir(hostRoot)
			if readErr != nil {
				t.Fatalf("read host root: %v", readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("rejected path created host entries: %v", entries)
			}
		})
	}
}

// Rationale: a canonical absolute path is not sufficient authorization. The
// Agent may write only below the exact stable-id environment volume selected
// by the Controller, never an arbitrary host path or an owner of another kind.
func TestFileMaterializerRejectsPathsOutsideCanonicalEnvironmentVolume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		volumeDir string
	}{
		{name: "arbitrary host path", volumeDir: "/etc"},
		{
			name:      "wrong managed root",
			volumeDir: "/var/lib/groundplane/vol/" + testTenantID + "/" + testProjectID + "/" + testEnvironmentID,
		},
		{
			name:      "missing environment",
			volumeDir: "/infra/vol/" + testTenantID + "/" + testProjectID,
		},
		{name: "extra component", volumeDir: testVolumeDir + "/extra"},
		{
			name:      "project in tenant position",
			volumeDir: "/infra/vol/" + testProjectID + "/" + testProjectID + "/" + testEnvironmentID,
		},
		{
			name:      "environment in project position",
			volumeDir: "/infra/vol/" + testTenantID + "/" + testEnvironmentID + "/" + testEnvironmentID,
		},
		{
			name:      "tenant in environment position",
			volumeDir: "/infra/vol/" + testTenantID + "/" + testProjectID + "/" + testTenantID,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			hostRoot := t.TempDir()
			content := []byte("must-not-be-written")
			err := testFileMaterializer(hostRoot).materialize(
				context.Background(), test.volumeDir, "config/secret", content,
			)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("materialize() error = %v, want validation failure", err)
			}
			if !allBytesZero(content) {
				t.Fatal("rejected plaintext was not cleared")
			}
			entries, readErr := os.ReadDir(hostRoot)
			if readErr != nil || len(entries) != 0 {
				t.Fatalf("rejected volume created host entries %v, error = %v", entries, readErr)
			}
		})
	}
}

// Rationale: no symlink at the volume, parent, or destination boundary may
// redirect a secret-bearing write to another inode or directory.
func TestFileMaterializerRefusesSymlinkSubstitution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prepare func(t *testing.T, hostRoot, outside string)
	}{
		{
			name: "volume ancestor",
			prepare: func(t *testing.T, hostRoot, outside string) {
				t.Helper()
				if err := os.Symlink(outside, filepath.Join(hostRoot, "infra")); err != nil {
					t.Fatalf("create volume symlink: %v", err)
				}
			},
		},
		{
			name: "destination parent",
			prepare: func(t *testing.T, hostRoot, outside string) {
				t.Helper()
				volume := materializedTestPath(hostRoot, "")
				if err := os.MkdirAll(volume, 0o700); err != nil {
					t.Fatalf("create volume: %v", err)
				}
				if err := os.Symlink(outside, filepath.Join(volume, "config")); err != nil {
					t.Fatalf("create parent symlink: %v", err)
				}
			},
		},
		{
			name: "destination",
			prepare: func(t *testing.T, hostRoot, outside string) {
				t.Helper()
				parent := materializedTestPath(hostRoot, "config")
				if err := os.MkdirAll(parent, 0o700); err != nil {
					t.Fatalf("create destination parent: %v", err)
				}
				if err := os.Symlink(
					filepath.Join(outside, "keep"),
					filepath.Join(parent, "secret"),
				); err != nil {
					t.Fatalf("create destination symlink: %v", err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			hostRoot := t.TempDir()
			outside := t.TempDir()
			marker := filepath.Join(outside, "keep")
			if err := os.WriteFile(marker, []byte("untouched"), 0o600); err != nil {
				t.Fatalf("write outside marker: %v", err)
			}
			test.prepare(t, hostRoot, outside)
			content := []byte("sensitive")
			err := testFileMaterializer(hostRoot).materialize(
				context.Background(), testVolumeDir, "config/secret", content,
			)
			if err == nil {
				t.Fatal("materialize() error = nil, want symlink refusal")
			}
			got, readErr := os.ReadFile(marker)
			if readErr != nil || string(got) != "untouched" {
				t.Fatalf("outside marker = %q, error = %v; want untouched", got, readErr)
			}
			if !allBytesZero(content) {
				t.Fatal("rejected plaintext was not cleared")
			}
		})
	}
}

// Rationale: a failed atomic publish must retain the prior durable file and
// remove the temporary plaintext artifact.
func TestFileMaterializerPreservesDestinationAndCleansTemporaryOnRenameFailure(t *testing.T) {
	t.Parallel()

	hostRoot := t.TempDir()
	parent := materializedTestPath(hostRoot, "secrets")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatalf("create destination parent: %v", err)
	}
	destination := filepath.Join(parent, ".env.env_01")
	if err := os.WriteFile(destination, []byte("OLD=\"value\"\n"), 0o600); err != nil {
		t.Fatalf("write existing destination: %v", err)
	}

	materializer := testFileMaterializer(hostRoot)
	materializer.rename = func(*os.Root, string, string) error {
		return errors.New("rename blocked")
	}
	content := []byte("NEW=\"secret\"\n")
	err := materializer.materialize(
		context.Background(), testVolumeDir, "secrets/.env.env_01", content,
	)
	if err == nil {
		t.Fatal("materialize() error = nil, want rename failure")
	}
	got, readErr := os.ReadFile(destination)
	if readErr != nil || string(got) != "OLD=\"value\"\n" {
		t.Fatalf("destination = %q, error = %v; want old content", got, readErr)
	}
	entries, readErr := os.ReadDir(parent)
	if readErr != nil {
		t.Fatalf("read destination parent: %v", readErr)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), temporaryFilePrefix) {
			t.Fatalf("plaintext temporary file remained: %s", entry.Name())
		}
	}
	if !allBytesZero(content) {
		t.Fatal("failed plaintext was not cleared")
	}
}

// Rationale: cancellation before publication must leave the prior file intact
// and clear the unpublished plaintext.
func TestFileMaterializerHonorsCancellationWithoutPublishingPartialContent(t *testing.T) {
	t.Parallel()

	hostRoot := t.TempDir()
	parent := materializedTestPath(hostRoot, "config")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatalf("create destination parent: %v", err)
	}
	destination := filepath.Join(parent, "secret")
	if err := os.WriteFile(destination, []byte("old"), 0o600); err != nil {
		t.Fatalf("write existing destination: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	content := bytes.Repeat([]byte("secret"), 1024)
	err := testFileMaterializer(hostRoot).materialize(
		ctx, testVolumeDir, "config/secret", content,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("materialize() error = %v, want context canceled", err)
	}
	got, readErr := os.ReadFile(destination)
	if readErr != nil || string(got) != "old" {
		t.Fatalf("destination = %q, error = %v; want old content", got, readErr)
	}
	if !allBytesZero(content) {
		t.Fatal("canceled plaintext was not cleared")
	}
}

// Rationale: an untrusted owner or writable ancestor could substitute the
// destination between checks, so either condition must fail before creation.
func TestFileMaterializerRefusesWrongOwnerAndWritableAncestor(t *testing.T) {
	t.Parallel()

	t.Run("wrong owner", func(t *testing.T) {
		t.Parallel()
		hostRoot := t.TempDir()
		materializer := newFileMaterializer(hostRoot, uint32(os.Geteuid())^1, rand.Reader)
		content := []byte("sensitive")
		if err := materializer.materialize(
			context.Background(), testVolumeDir, "config/secret", content,
		); err == nil {
			t.Fatal("materialize() error = nil, want wrong-owner refusal")
		}
		if !allBytesZero(content) {
			t.Fatal("rejected plaintext was not cleared")
		}
		entries, err := os.ReadDir(hostRoot)
		if err != nil || len(entries) != 0 {
			t.Fatalf("wrong-owner attempt created entries %v, error = %v", entries, err)
		}
	})

	t.Run("writable ancestor", func(t *testing.T) {
		t.Parallel()
		hostRoot := t.TempDir()
		ancestor := filepath.Join(hostRoot, "infra")
		if err := os.Mkdir(ancestor, 0o700); err != nil {
			t.Fatalf("create ancestor: %v", err)
		}
		if err := os.Chmod(ancestor, 0o777); err != nil {
			t.Fatalf("make ancestor writable: %v", err)
		}
		content := []byte("sensitive")
		if err := testFileMaterializer(hostRoot).materialize(
			context.Background(), testVolumeDir, "config/secret", content,
		); err == nil {
			t.Fatal("materialize() error = nil, want writable-ancestor refusal")
		}
		if _, err := os.Stat(filepath.Join(ancestor, "vol")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("writable ancestor gained child, stat error = %v", err)
		}
		if !allBytesZero(content) {
			t.Fatal("rejected plaintext was not cleared")
		}
	})
}

// Rationale: retries targeting one deterministic path must not consume or
// overwrite one another's temporary plaintext during concurrent execution.
func TestFileMaterializerSerializesConcurrentReplacements(t *testing.T) {
	t.Parallel()

	hostRoot := t.TempDir()
	materializer := testFileMaterializer(hostRoot)
	originalRename := materializer.rename
	renameEntered := make(chan struct{})
	releaseRename := make(chan struct{})
	var first sync.Once
	materializer.rename = func(root *os.Root, oldName, newName string) error {
		first.Do(func() {
			close(renameEntered)
			<-releaseRename
		})
		return originalRename(root, oldName, newName)
	}

	firstContent := []byte("VALUE=\"first\"\n")
	secondContent := []byte("VALUE=\"second\"\n")
	results := make(chan error, 2)
	go func() {
		results <- materializer.materialize(
			context.Background(), testVolumeDir, "secrets/.env.env_01", firstContent,
		)
	}()
	<-renameEntered
	go func() {
		results <- materializer.materialize(
			context.Background(), testVolumeDir, "secrets/.env.env_01", secondContent,
		)
	}()
	close(releaseRename)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("concurrent materialize() error = %v", err)
		}
	}

	got, err := os.ReadFile(materializedTestPath(hostRoot, "secrets/.env.env_01"))
	if err != nil || string(got) != "VALUE=\"second\"\n" {
		t.Fatalf("final content = %q, error = %v; want second replacement", got, err)
	}
	if !allBytesZero(firstContent) || !allBytesZero(secondContent) {
		t.Fatal("concurrent materialize() retained plaintext")
	}
}

// Rationale: cancellation racing with rename must not skip the post-rename
// directory sync, even though the caller still receives cancellation.
func TestFileMaterializerFinishesDurabilityAfterCommitCancellation(t *testing.T) {
	t.Parallel()

	hostRoot := t.TempDir()
	materializer := testFileMaterializer(hostRoot)
	originalRename := materializer.rename
	ctx, cancel := context.WithCancel(context.Background())
	materializer.rename = func(root *os.Root, oldName, newName string) error {
		err := originalRename(root, oldName, newName)
		cancel()
		return err
	}
	content := []byte("VALUE=\"committed\"\n")
	err := materializer.materialize(
		ctx, testVolumeDir, "secrets/.env.env_01", content,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("materialize() error = %v, want context canceled after durable commit", err)
	}
	got, readErr := os.ReadFile(materializedTestPath(hostRoot, "secrets/.env.env_01"))
	if readErr != nil || string(got) != "VALUE=\"committed\"\n" {
		t.Fatalf("committed content = %q, error = %v", got, readErr)
	}
	if !allBytesZero(content) {
		t.Fatal("committed plaintext was not cleared")
	}
}

func testFileMaterializer(hostRoot string) *fileMaterializer {
	return newFileMaterializer(hostRoot, uint32(os.Geteuid()), rand.Reader)
}

func materializedTestPath(hostRoot, relative string) string {
	return filepath.Join(
		hostRoot,
		strings.TrimPrefix(testVolumeDir, "/"),
		filepath.FromSlash(relative),
	)
}

func allBytesZero(value []byte) bool {
	for _, b := range value {
		if b != 0 {
			return false
		}
	}
	return true
}
