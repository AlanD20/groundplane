//go:build linux

package entrymaterializer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// Rationale: production construction must expose only the fixed helper mount;
// descriptor injection remains private test infrastructure.
func TestRootPathIsFixedHelperMount(t *testing.T) {
	t.Parallel()

	if rootPath != "/run/groundplane/materialize" {
		t.Fatalf("rootPath = %q, want fixed helper mount", rootPath)
	}
}

// Rationale: a missing context is an internal wiring defect and must fail
// before fixed-root access, descriptor mutation, or plaintext reader access.
func TestNilContextFailsBeforeEveryBoundary(t *testing.T) {
	t.Parallel()

	limits := entrymaterialization.Limits{MaxContentBytes: 64, MaxDestinationBytes: 4095}
	source := &trackingReadCloser{}
	if err := Run(nil, source, limits); !isInternal(err) {
		t.Fatalf("Run(nil) error = %v, want internal failure", err)
	}
	if !source.closed || source.read {
		t.Fatalf(
			"Run(nil) source state = closed:%t read:%t, want closed before read",
			source.closed,
			source.read,
		)
	}
	if materializer, err := openMaterializer(nil); materializer != nil || !isInternal(err) {
		t.Fatalf("openMaterializer(nil) = (%v, %v), want internal failure", materializer, err)
	}
	if materializer, err := newMaterializer(
		nil,
		nil,
		nil,
		0,
		0,
		productionLinuxOps(),
	); materializer != nil || !isInternal(err) {
		t.Fatalf("newMaterializer(nil) = (%v, %v), want internal failure", materializer, err)
	}
	materializer, _ := testMaterializer(t)
	header := testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, []byte("secret"))
	if err := materializer.materialize(nil, header, panicReader{}); !isInternal(err) {
		t.Fatalf("materialize(nil) error = %v, want internal failure", err)
	}
	if err := materializer.close(nil); !isInternal(err) {
		t.Fatalf("close(nil) error = %v, want internal failure", err)
	}
}

// Rationale: cancellation observed after the secure root probe is an operation
// failure, but a subsequent descriptor-close failure takes opaque internal
// precedence and must not remain classifiable as cancellation.
func TestConstructorCloseFailureSuppressesPostProbeCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	injected := errors.New("injected rejected-root close failure")
	production := productionLinuxOps()
	ops := production
	closeCalls := 0
	ops.openat2 = func(directory int, name string, how *unix.OpenHow) (int, error) {
		fd, err := production.openat2(directory, name, how)
		if err == nil {
			cancel()
		}
		return fd, err
	}
	ops.close = func(fd int) error {
		closeCalls++
		closeErr := production.close(fd)
		if closeCalls == 2 {
			return errors.Join(closeErr, injected)
		}
		return closeErr
	}
	rootPath := t.TempDir()
	if err := os.Chmod(rootPath, 0o700); err != nil {
		t.Fatalf("set root mode: %v", err)
	}
	root, err := os.Open(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	materializer, createErr := newMaterializer(
		ctx,
		root,
		bytes.NewReader(bytes.Repeat([]byte{0x5a}, 64)),
		testUID(),
		testGID(),
		ops,
	)
	if closeErr := root.Close(); closeErr != nil {
		t.Fatalf("close source root: %v", closeErr)
	}
	if materializer != nil {
		t.Fatal("newMaterializer() returned a materializer after cancellation")
	}
	if !isInternal(createErr) || !errors.Is(createErr, injected) ||
		errors.Is(createErr, context.Canceled) ||
		errors.Is(createErr, context.DeadlineExceeded) {
		t.Fatalf("newMaterializer() error = %v, want close-only internal failure", createErr)
	}
}

// Rationale: common Header has an intentional zero value, but only values
// produced by its validating constructor or decoder may reach filesystem work.
func TestMaterializeRejectsZeroHeaderBeforeReaderAccess(t *testing.T) {
	t.Parallel()

	helper, root := testMaterializer(t)
	err := helper.materialize(
		context.Background(),
		entrymaterialization.Header{},
		panicReader{},
	)
	if !isInternal(err) {
		t.Fatalf("Materialize(zero header) error = %v, want internal failure", err)
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		t.Fatalf("read root: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("root entries = %v, want no filesystem mutation", entries)
	}
}

// Rationale: one canonical frame must remain unpublished until the common
// decoder accepts content length, digest, terminal record, source EOF, and close.
func TestRunPublishesOnlyAfterCompleteFrameVerification(t *testing.T) {
	t.Parallel()

	helper, root := testMaterializer(t)
	content := []byte("streamed-secret-material")
	header := testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)
	limits := testLimits(content)
	frame := testFrame(t, header, content, limits)
	opener := func(context.Context) (*materializer, error) { return helper, nil }

	if err := run(
		context.Background(),
		io.NopCloser(bytes.NewReader(frame)),
		limits,
		opener,
	); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	destination := filepath.Join(root, "files", "secret")
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("destination = %q, want exact streamed content", got)
	}
	assertNoTemporary(t, filepath.Dir(destination))
}

// Rationale: source cleanup is part of common frame acceptance, so a close
// failure after streaming must abort the unpublished temporary.
func TestRunAbortsPreparedFileWhenFrameSourceCloseFails(t *testing.T) {
	t.Parallel()

	helper, root := testMaterializer(t)
	content := []byte("streamed-secret-material")
	header := testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)
	limits := testLimits(content)
	frame := testFrame(t, header, content, limits)
	source := &closeFailingReader{Reader: bytes.NewReader(frame)}
	opener := func(context.Context) (*materializer, error) { return helper, nil }

	if err := run(context.Background(), source, limits, opener); err == nil {
		t.Fatal("run() error = nil, want source-close failure")
	}
	destination := filepath.Join(root, "files", "secret")
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination stat error = %v, want unpublished", err)
	}
	assertNoTemporary(t, filepath.Dir(destination))
}

// Rationale: test-only opener injection must not weaken source ownership; a
// missing opener still closes the authenticated frame source on callback exit.
func TestRunClosesFrameSourceWhenOpenerIsMissing(t *testing.T) {
	t.Parallel()

	content := []byte("streamed-secret-material")
	header := testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)
	limits := testLimits(content)
	source := &trackingReadCloser{Reader: bytes.NewReader(testFrame(t, header, content, limits))}

	if err := run(context.Background(), source, limits, nil); !isInternal(err) {
		t.Fatalf("run(nil opener) error = %v, want internal failure", err)
	}
	if !source.closed {
		t.Fatal("run(nil opener) did not close owned frame source")
	}
}

// Rationale: a valid request must atomically replace the destination with the
// exact bytes and declared identity while leaving only root-owned 0700 parents.
func TestMaterializePublishesExactFile(t *testing.T) {
	t.Parallel()

	materializer, root := testMaterializer(t)
	destination := filepath.Join(root, "nested", "settings")
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		t.Fatalf("create destination parent: %v", err)
	}
	if err := os.WriteFile(destination, []byte("old"), 0o600); err != nil {
		t.Fatalf("write old destination: %v", err)
	}
	content := []byte("new-secret-material")
	header := testHeader(t, "nested/settings", entrymaterialization.OutputSecretFile, content)

	if err := materializer.materialize(
		context.Background(),
		header,
		bytes.NewReader(content),
	); err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("destination = %q, want exact content", got)
	}
	assertMetadata(t, destination, header.UID(), header.GID(), uint32(header.Mode()), 1)
	assertMetadata(t, filepath.Join(root, "nested"), testUID(), testGID(), 0o700, 0)
	assertNoTemporary(t, filepath.Dir(destination))
}

// Rationale: path resolution must never follow a symbolic-link ancestor into
// another location, even when the target otherwise satisfies file metadata.
func TestMaterializeRefusesSymlinkAncestor(t *testing.T) {
	t.Parallel()

	materializer, root := testMaterializer(t)
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "redirect")); err != nil {
		t.Fatalf("create ancestor symlink: %v", err)
	}
	content := []byte("must-stay-inside")
	header := testHeader(t, "redirect/secret", entrymaterialization.OutputSecretFile, content)

	if err := materializer.materialize(
		context.Background(),
		header,
		bytes.NewReader(content),
	); err == nil {
		t.Fatal("Materialize() error = nil, want symlink refusal")
	}
	if _, err := os.Lstat(filepath.Join(outside, "secret")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("outside destination stat error = %v, want not exist", err)
	}
}

// Rationale: an existing destination with multiple names is not safe to
// replace because its identity no longer matches the single-file invariant.
func TestMaterializeRefusesHardLinkedDestination(t *testing.T) {
	t.Parallel()

	materializer, root := testMaterializer(t)
	directory := filepath.Join(root, "files")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("create directory: %v", err)
	}
	destination := filepath.Join(directory, "secret")
	if err := os.WriteFile(destination, []byte("old"), 0o600); err != nil {
		t.Fatalf("write destination: %v", err)
	}
	if err := os.Link(destination, filepath.Join(directory, "alias")); err != nil {
		t.Fatalf("link destination: %v", err)
	}
	content := []byte("replacement")
	header := testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)

	if err := materializer.materialize(
		context.Background(),
		header,
		bytes.NewReader(content),
	); err == nil {
		t.Fatal("Materialize() error = nil, want hard-link refusal")
	}
	got, err := os.ReadFile(destination)
	if err != nil || string(got) != "old" {
		t.Fatalf("destination = %q, error = %v, want unchanged", got, err)
	}
}

// Rationale: a final name is safe only when it is one regular file with the
// exact declared owner and mode; every other inode state must fail before read.
func TestMaterializeRefusesUnsafeExistingDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		setup  func(*testing.T, string)
		header func(*testing.T, []byte) entrymaterialization.Header
	}{
		{
			name: "symlink",
			setup: func(t *testing.T, destination string) {
				t.Helper()
				if err := os.Symlink("outside", destination); err != nil {
					t.Fatalf("create destination symlink: %v", err)
				}
			},
			header: func(t *testing.T, content []byte) entrymaterialization.Header {
				return testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)
			},
		},
		{
			name: "fifo",
			setup: func(t *testing.T, destination string) {
				t.Helper()
				if err := unix.Mkfifo(destination, 0o600); err != nil {
					t.Fatalf("create destination fifo: %v", err)
				}
			},
			header: func(t *testing.T, content []byte) entrymaterialization.Header {
				return testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)
			},
		},
		{
			name: "owner mismatch",
			setup: func(t *testing.T, destination string) {
				t.Helper()
				if err := os.WriteFile(destination, []byte("old"), 0o600); err != nil {
					t.Fatalf("write destination: %v", err)
				}
			},
			header: func(t *testing.T, content []byte) entrymaterialization.Header {
				spec := testHeaderSpec(
					"files/secret",
					entrymaterialization.OutputSecretFile,
					content,
				)
				spec.UID++
				return mustHeader(t, spec)
			},
		},
		{
			name: "mode mismatch",
			setup: func(t *testing.T, destination string) {
				t.Helper()
				if err := os.WriteFile(destination, []byte("old"), 0o644); err != nil {
					t.Fatalf("write destination: %v", err)
				}
			},
			header: func(t *testing.T, content []byte) entrymaterialization.Header {
				return testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			helper, root := testMaterializer(t)
			directory := filepath.Join(root, "files")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatalf("create directory: %v", err)
			}
			destination := filepath.Join(directory, "secret")
			test.setup(t, destination)
			content := []byte("replacement")

			if err := helper.materialize(
				context.Background(),
				test.header(t, content),
				panicReader{},
			); !isInternal(err) {
				t.Fatalf("materialize() error = %v, want internal refusal", err)
			}
		})
	}
}

// Rationale: openat2 is the traversal security boundary; mount transitions,
// magic links, and unsupported kernels must fail with every resolve bit set.
func TestMaterializerFailsClosedWhenOpenat2PolicyCannotResolve(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "nested mount", err: unix.EXDEV},
		{name: "magic link", err: unix.ELOOP},
		{name: "unsupported kernel", err: unix.ENOSYS},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rootPath := t.TempDir()
			if err := os.Chown(rootPath, os.Geteuid(), os.Getegid()); err != nil {
				t.Fatalf("set root owner: %v", err)
			}
			if err := os.Chmod(rootPath, 0o700); err != nil {
				t.Fatalf("set root mode: %v", err)
			}
			root, err := os.Open(rootPath)
			if err != nil {
				t.Fatalf("open root: %v", err)
			}
			defer root.Close()
			ops := productionLinuxOps()
			var observed unix.OpenHow
			ops.openat2 = func(_ int, _ string, how *unix.OpenHow) (int, error) {
				observed = *how
				return -1, test.err
			}

			helper, err := newMaterializer(
				context.Background(),
				root,
				bytes.NewReader(bytes.Repeat([]byte{1}, 64)),
				testUID(),
				testGID(),
				ops,
			)
			if helper != nil || !isInternal(err) {
				t.Fatalf("newMaterializer() = (%v, %v), want fail closed", helper, err)
			}
			if observed.Resolve != beneathPolicy {
				t.Fatalf("openat2 resolve = %#x, want %#x", observed.Resolve, beneathPolicy)
			}
		})
	}
}

// Rationale: a killed prior helper may leave one single-link temporary; the
// next durably ordered invocation must remove and sync it before publishing.
func TestMaterializeReconcilesRegularOrphan(t *testing.T) {
	t.Parallel()

	materializer, root := testMaterializer(t)
	directory := filepath.Join(root, "files")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("create directory: %v", err)
	}
	orphan := filepath.Join(directory, entrymaterialization.TemporaryPrefix+"orphan")
	if err := os.WriteFile(orphan, []byte("stale-plaintext"), 0o600); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	content := []byte("replacement")
	header := testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)

	if err := materializer.materialize(
		context.Background(),
		header,
		bytes.NewReader(content),
	); err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}
	if _, err := os.Lstat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan stat error = %v, want not exist", err)
	}
	assertNoTemporary(t, directory)
}

// Rationale: an orphan with another name may still expose plaintext, so the
// helper must preserve it for inspection and refuse all further mutation.
func TestMaterializeRefusesHardLinkedOrphan(t *testing.T) {
	t.Parallel()

	materializer, root := testMaterializer(t)
	directory := filepath.Join(root, "files")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("create directory: %v", err)
	}
	orphan := filepath.Join(directory, entrymaterialization.TemporaryPrefix+"orphan")
	if err := os.WriteFile(orphan, []byte("stale-plaintext"), 0o600); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	if err := os.Link(orphan, filepath.Join(directory, "alias")); err != nil {
		t.Fatalf("link orphan: %v", err)
	}
	content := []byte("replacement")
	header := testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)

	if err := materializer.materialize(
		context.Background(),
		header,
		bytes.NewReader(content),
	); err == nil {
		t.Fatal("Materialize() error = nil, want unsafe-orphan refusal")
	}
	if _, err := os.Lstat(orphan); err != nil {
		t.Fatalf("orphan must remain for inspection: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(directory, "secret")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination stat error = %v, want not exist", err)
	}
}

// Rationale: framing length and digest are independent integrity boundaries;
// each mismatch must leave the old destination and no plaintext temporary.
func TestMaterializeRejectsContentFramingMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		header  func(*testing.T, []byte) entrymaterialization.Header
		content []byte
	}{
		{name: "early eof", header: func(t *testing.T, content []byte) entrymaterialization.Header {
			spec := testHeaderSpec("files/secret", entrymaterialization.OutputSecretFile, content)
			spec.Length++
			return mustHeader(t, spec)
		}, content: []byte("short")},
		{
			name: "extra bytes",
			header: func(t *testing.T, content []byte) entrymaterialization.Header {
				spec := testHeaderSpec(
					"files/secret",
					entrymaterialization.OutputSecretFile,
					content,
				)
				spec.Length--
				return mustHeader(t, spec)
			},
			content: []byte("extra"),
		},
		{
			name: "digest mismatch",
			header: func(t *testing.T, content []byte) entrymaterialization.Header {
				spec := testHeaderSpec(
					"files/secret",
					entrymaterialization.OutputSecretFile,
					content,
				)
				spec.Digest[0] ^= 0xff
				return mustHeader(t, spec)
			},
			content: []byte("digest"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			materializer, root := testMaterializer(t)
			directory := filepath.Join(root, "files")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatalf("create directory: %v", err)
			}
			destination := filepath.Join(directory, "secret")
			if err := os.WriteFile(destination, []byte("old"), 0o600); err != nil {
				t.Fatalf("write destination: %v", err)
			}

			err := materializer.materialize(
				context.Background(),
				test.header(t, test.content),
				bytes.NewReader(test.content),
			)
			if err == nil {
				t.Fatal("Materialize() error = nil, want framing refusal")
			}
			got, readErr := os.ReadFile(destination)
			if readErr != nil || string(got) != "old" {
				t.Fatalf("destination = %q, error = %v, want unchanged", got, readErr)
			}
			assertNoTemporary(t, directory)
		})
	}
}

// Rationale: cancellation before rename must remove plaintext and preserve the
// old destination without converting cancellation into an internal failure.
func TestMaterializeCancellationBeforeRenamePreservesDestination(t *testing.T) {
	t.Parallel()

	materializer, root := testMaterializer(t)
	directory := filepath.Join(root, "files")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("create directory: %v", err)
	}
	destination := filepath.Join(directory, "secret")
	if err := os.WriteFile(destination, []byte("old"), 0o600); err != nil {
		t.Fatalf("write destination: %v", err)
	}
	content := []byte("cancelled-content")
	header := testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)
	ctx, cancel := context.WithCancel(context.Background())
	reader := &cancelingReader{reader: bytes.NewReader(content), cancel: cancel}

	err := materializer.materialize(ctx, header, reader)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Materialize() error = %v, want context cancellation", err)
	}
	got, readErr := os.ReadFile(destination)
	if readErr != nil || string(got) != "old" {
		t.Fatalf("destination = %q, error = %v, want unchanged", got, readErr)
	}
	assertNoTemporary(t, directory)
}

// Rationale: rename is the publication linearization point. Cancellation that
// races with it cannot roll publication back, and the containing directory
// must still be synced before cancellation is returned.
func TestMaterializeCancellationAtRenameStillSyncsPublication(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	production := productionLinuxOps()
	publicationRenamed := false
	syncedAfterCancellation := false
	ops := production
	ops.renameat = func(oldDirectory int, oldName string, newDirectory int, newName string) error {
		err := production.renameat(oldDirectory, oldName, newDirectory, newName)
		if err == nil {
			publicationRenamed = true
			cancel()
		}
		return err
	}
	ops.fsync = func(fd int) error {
		if publicationRenamed && ctx.Err() != nil {
			syncedAfterCancellation = true
		}
		return production.fsync(fd)
	}
	materializer, root := testMaterializerWithOps(t, ops)
	directory := filepath.Join(root, "files")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("create directory: %v", err)
	}
	content := []byte("published-at-cancellation")
	header := testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)

	err := materializer.materialize(ctx, header, bytes.NewReader(content))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("materialize() error = %v, want context cancellation", err)
	}
	if !syncedAfterCancellation {
		t.Fatal("published directory was not synced after cancellation")
	}
	got, readErr := os.ReadFile(filepath.Join(directory, "secret"))
	if readErr != nil || !bytes.Equal(got, content) {
		t.Fatalf("published content = %q, error = %v", got, readErr)
	}
	assertNoTemporary(t, directory)
}

// Rationale: a successful rename without a successful directory sync is not
// a durable success and must be reported as an internal infrastructure error.
func TestMaterializeReportsPublishedDirectorySyncFailure(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected directory sync failure")
	production := productionLinuxOps()
	published := false
	ops := production
	ops.renameat = func(oldDirectory int, oldName string, newDirectory int, newName string) error {
		err := production.renameat(oldDirectory, oldName, newDirectory, newName)
		if err == nil {
			published = true
		}
		return err
	}
	ops.fsync = func(fd int) error {
		if err := production.fsync(fd); err != nil {
			return err
		}
		if published {
			return injected
		}
		return nil
	}
	materializer, root := testMaterializerWithOps(t, ops)
	directory := filepath.Join(root, "files")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("create directory: %v", err)
	}
	content := []byte("published-but-sync-failed")
	header := testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)

	err := materializer.materialize(context.Background(), header, bytes.NewReader(content))
	if !isInternal(err) || !errors.Is(err, injected) {
		t.Fatalf("materialize() error = %v, want injected internal failure", err)
	}
	got, readErr := os.ReadFile(filepath.Join(directory, "secret"))
	if readErr != nil || !bytes.Equal(got, content) {
		t.Fatalf("published content = %q, error = %v", got, readErr)
	}
	assertNoTemporary(t, directory)
}

// Rationale: mandatory plaintext cleanup runs without caller cancellation;
// close, unlink, and directory-sync failures each take internal precedence.
func TestMaterializeReportsEveryTemporaryCleanupFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		inject func(*linuxOps, linuxOps, *bool, error)
	}{
		{
			name: "close",
			inject: func(ops *linuxOps, production linuxOps, armed *bool, injected error) {
				fired := false
				ops.close = func(fd int) error {
					err := production.close(fd)
					if *armed && !fired {
						fired = true
						return errors.Join(err, injected)
					}
					return err
				}
			},
		},
		{
			name: "unlink",
			inject: func(ops *linuxOps, production linuxOps, armed *bool, injected error) {
				fired := false
				ops.unlinkat = func(directory int, name string, flags int) error {
					if *armed && !fired {
						fired = true
						return injected
					}
					return production.unlinkat(directory, name, flags)
				}
			},
		},
		{
			name: "fsync",
			inject: func(ops *linuxOps, production linuxOps, armed *bool, injected error) {
				fired := false
				ops.fsync = func(fd int) error {
					err := production.fsync(fd)
					if *armed && !fired {
						fired = true
						return errors.Join(err, injected)
					}
					return err
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			injected := errors.New("injected " + test.name + " cleanup failure")
			production := productionLinuxOps()
			ops := production
			armed := false
			test.inject(&ops, production, &armed, injected)
			materializer, root := testMaterializerWithOps(t, ops)
			directory := filepath.Join(root, "files")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatalf("create directory: %v", err)
			}
			content := []byte("cancel-before-write")
			header := testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)
			ctx, cancel := context.WithCancel(context.Background())
			reader := &armingCancelReader{
				reader: bytes.NewReader(content),
				arm: func() {
					armed = true
					cancel()
				},
			}

			err := materializer.materialize(ctx, header, reader)
			if !isInternal(err) || !errors.Is(err, injected) || errors.Is(err, context.Canceled) {
				t.Fatalf("materialize() error = %v, want injected cleanup failure only", err)
			}
		})
	}
}

// Rationale: plaintext-derived digest state must be destroyed after both the
// write hash and verification hash, including an early untrusted-reader error.
func TestMaterializeDestroysEveryPlaintextHasher(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		var hashers []*trackingHasher
		ops := productionLinuxOps()
		ops.newHasher = func() digestHasher {
			hasher := &trackingHasher{inner: entrymaterialization.NewHasher()}
			hashers = append(hashers, hasher)
			return hasher
		}
		materializer, _ := testMaterializerWithOps(t, ops)
		content := []byte("hash-state-success")
		header := testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)
		if err := materializer.materialize(
			context.Background(),
			header,
			bytes.NewReader(content),
		); err != nil {
			t.Fatalf("materialize() error = %v", err)
		}
		if len(hashers) != 2 {
			t.Fatalf("hasher count = %d, want write and verification hashers", len(hashers))
		}
		for index, hasher := range hashers {
			if !hasher.destroyed || !hasher.verified {
				t.Fatalf(
					"hasher %d = destroyed:%t verified:%t",
					index,
					hasher.destroyed,
					hasher.verified,
				)
			}
		}
	})

	t.Run("reader failure", func(t *testing.T) {
		t.Parallel()
		var hashers []*trackingHasher
		ops := productionLinuxOps()
		ops.newHasher = func() digestHasher {
			hasher := &trackingHasher{inner: entrymaterialization.NewHasher()}
			hashers = append(hashers, hasher)
			return hasher
		}
		materializer, _ := testMaterializerWithOps(t, ops)
		content := []byte("hash-state-failure")
		header := testHeader(t, "files/secret", entrymaterialization.OutputSecretFile, content)
		err := materializer.materialize(
			context.Background(),
			header,
			&failingReader{err: errors.New("untrusted reader failure")},
		)
		if err == nil {
			t.Fatal("materialize() error = nil, want reader failure")
		}
		if len(hashers) != 1 || !hashers[0].destroyed || hashers[0].verified {
			t.Fatalf("hashers = %#v, want one destroyed unverified write hasher", hashers)
		}
	})
}

// Rationale: typed removal must unlink only a regular file with the exact
// sealed metadata, remain idempotent when absent, and refuse changed metadata.
func TestMaterializeRemovalIsExactAndIdempotent(t *testing.T) {
	t.Parallel()
	materializer, root := testMaterializer(t)
	directory := filepath.Join(root, "files")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("create directory: %v", err)
	}
	destination := filepath.Join(directory, "config")
	if err := os.WriteFile(destination, []byte("retired"), 0o444); err != nil {
		t.Fatalf("write destination: %v", err)
	}
	if err := os.Chown(destination, int(testUID()), int(testGID())); err != nil {
		t.Fatalf("set destination ownership: %v", err)
	}
	header := testHeader(t, "files/config", entrymaterialization.OutputRemovePlainFile, nil)
	if err := materializer.materialize(context.Background(), header, bytes.NewReader(nil)); err != nil {
		t.Fatalf("materialize(removal) error = %v", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed destination stat error = %v", err)
	}
	if err := materializer.materialize(context.Background(), header, bytes.NewReader(nil)); err != nil {
		t.Fatalf("materialize(removal replay) error = %v", err)
	}
	if err := materializer.materialize(
		context.Background(),
		testHeader(t, "missing/parent/config", entrymaterialization.OutputRemovePlainFile, nil),
		bytes.NewReader(nil),
	); err != nil {
		t.Fatalf("materialize(missing parent removal) error = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removal created a missing parent: %v", err)
	}
	if err := os.WriteFile(destination, []byte("changed-mode"), 0o600); err != nil {
		t.Fatalf("write mismatched destination: %v", err)
	}
	if err := os.Chown(destination, int(testUID()), int(testGID())); err != nil {
		t.Fatalf("set mismatched destination ownership: %v", err)
	}
	if err := materializer.materialize(
		context.Background(), header, bytes.NewReader(nil),
	); !isInternal(err) {
		t.Fatalf("materialize(mismatched removal) error = %v", err)
	}
	if _, err := os.Lstat(destination); err != nil {
		t.Fatalf("mismatched destination was removed: %v", err)
	}
}

// Rationale: cleanup precedence changes classification, not causality; every
// operation and cleanup failure must remain discoverable through errors.Is.
func TestCleanupPrecedenceRetainsEveryFailure(t *testing.T) {
	t.Parallel()

	operationErr := errors.New("operation failed")
	firstCleanup := errors.New("first cleanup")
	secondCleanup := errors.New("second cleanup")
	result := preferCleanupError(
		operationErr,
		wrapSystemError("first cleanup", firstCleanup),
	)
	result = preferCleanupError(
		result,
		wrapSystemError("second cleanup", secondCleanup),
	)

	if !isInternal(result) {
		t.Fatalf("cleanup result = %v, want internal classification", result)
	}
	for _, cause := range []error{operationErr, firstCleanup, secondCleanup} {
		if !errors.Is(result, cause) {
			t.Fatalf("cleanup result lost cause %v: %v", cause, result)
		}
	}
}

// Rationale: cleanup failure is the actionable public result; cancellation
// and deadline causes must not change its classification through errors.Is.
func TestCleanupPrecedenceSuppressesContextCauses(t *testing.T) {
	t.Parallel()

	for _, operationErr := range []error{context.Canceled, context.DeadlineExceeded} {
		cleanupErr := errors.New("cleanup failure")
		result := preferCleanupError(operationErr, wrapSystemError("cleanup", cleanupErr))
		if !isInternal(result) || !errors.Is(result, cleanupErr) ||
			errors.Is(result, operationErr) {
			t.Fatalf(
				"preferCleanupError(%v) = %v, want cleanup-only internal result",
				operationErr,
				result,
			)
		}
	}
}

// Rationale: cleanup dependencies run under WithoutCancel and therefore must
// never make their own direct or wrapped context sentinel externally visible.
func TestCleanupPrecedenceMakesCleanupContextCausesOpaque(t *testing.T) {
	t.Parallel()

	cleanupErrors := []error{
		context.Canceled,
		fmt.Errorf("wrapped cleanup cancellation: %w", context.Canceled),
		context.DeadlineExceeded,
		fmt.Errorf("wrapped cleanup deadline: %w", context.DeadlineExceeded),
	}
	for _, cleanupErr := range cleanupErrors {
		result := preferCleanupError(errors.New("operation failed"), cleanupErr)
		if !isInternal(result) || errors.Is(result, context.Canceled) ||
			errors.Is(result, context.DeadlineExceeded) {
			t.Fatalf("preferCleanupError(cleanup %v) = %v, want opaque internal error", cleanupErr, result)
		}
	}
}

// Rationale: stream failures are untrusted and may contain plaintext; helper
// errors must remain stable and secret-safe while cleaning the temporary.
func TestMaterializeDoesNotExposeReaderError(t *testing.T) {
	t.Parallel()

	materializer, root := testMaterializer(t)
	header := testHeader(
		t,
		"files/secret",
		entrymaterialization.OutputSecretFile,
		[]byte("expected"),
	)
	err := materializer.materialize(
		context.Background(),
		header,
		&failingReader{err: errors.New("plaintext-token-must-not-leak")},
	)
	if err == nil {
		t.Fatal("Materialize() error = nil, want stream failure")
	}
	if strings.Contains(err.Error(), "plaintext-token") {
		t.Fatalf("Materialize() error leaked reader detail: %v", err)
	}
	assertNoTemporary(t, filepath.Join(root, "files"))
}

func testMaterializer(t *testing.T) (*materializer, string) {
	return testMaterializerWithOps(t, productionLinuxOps())
}

func testMaterializerWithOps(t *testing.T, ops linuxOps) (*materializer, string) {
	t.Helper()
	rootPath := t.TempDir()
	if err := os.Chown(rootPath, os.Geteuid(), os.Getegid()); err != nil {
		t.Fatalf("set test root ownership: %v", err)
	}
	if err := os.Chmod(rootPath, 0o700); err != nil {
		t.Fatalf("set test root mode: %v", err)
	}
	root, err := os.Open(rootPath)
	if err != nil {
		t.Fatalf("open test root: %v", err)
	}
	materializer, err := newMaterializer(
		context.Background(),
		root,
		bytes.NewReader(bytes.Repeat([]byte{0x5a}, 4096)),
		testUID(),
		testGID(),
		ops,
	)
	if closeErr := root.Close(); closeErr != nil {
		t.Fatalf("close source root: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("newMaterializer() error = %v", err)
	}
	t.Cleanup(func() {
		materializer.lifecycleMu.Lock()
		isOpen := materializer.rootFD >= 0
		materializer.lifecycleMu.Unlock()
		if isOpen {
			if err := materializer.close(context.Background()); err != nil {
				t.Errorf("Close() error = %v", err)
			}
		}
	})
	return materializer, rootPath
}

func testHeader(
	t *testing.T,
	destination string,
	kind entrymaterialization.OutputKind,
	content []byte,
) entrymaterialization.Header {
	t.Helper()
	return mustHeader(t, testHeaderSpec(destination, kind, content))
}

func testHeaderSpec(
	destination string,
	kind entrymaterialization.OutputKind,
	content []byte,
) entrymaterialization.HeaderSpec {
	mode := entrymaterialization.ModeReadOnly
	uid := testUID()
	gid := testGID()
	if kind == entrymaterialization.OutputSecretFile || kind == entrymaterialization.OutputGeneratedEnv ||
		kind == entrymaterialization.OutputRemoveSecretFile ||
		kind == entrymaterialization.OutputRemoveGeneratedEnv {
		mode = entrymaterialization.ModePrivate
	}
	if kind == entrymaterialization.OutputGeneratedEnv || kind == entrymaterialization.OutputRemoveGeneratedEnv {
		uid = 0
		gid = 0
	}
	return entrymaterialization.HeaderSpec{
		TaskID:        "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepID:        "step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Generation:    7,
		Destination:   destination,
		OutputKind:    kind,
		UID:           uid,
		GID:           gid,
		Mode:          mode,
		Length:        uint64(len(content)),
		Digest:        entrymaterialization.DigestBytes(content),
	}
}

func mustHeader(t *testing.T, spec entrymaterialization.HeaderSpec) entrymaterialization.Header {
	t.Helper()
	header, err := entrymaterialization.NewHeader(spec)
	if err != nil {
		t.Fatalf("NewHeader() error = %v", err)
	}
	return header
}

func testLimits(content []byte) entrymaterialization.Limits {
	return entrymaterialization.Limits{
		MaxContentBytes:     uint64(len(content)),
		MaxDestinationBytes: 4095,
	}
}

func testFrame(
	t *testing.T,
	header entrymaterialization.Header,
	content []byte,
	limits entrymaterialization.Limits,
) []byte {
	t.Helper()
	var frame bytes.Buffer
	if err := entrymaterialization.Encode(
		context.Background(),
		&frame,
		header,
		entrymaterialization.OwnBytes(bytes.Clone(content)),
		limits,
	); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	return frame.Bytes()
}

func testUID() uint32 { return uint32(os.Geteuid()) }

func testGID() uint32 { return uint32(os.Getegid()) }

func assertNoTemporary(t *testing.T, directory string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read directory: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), entrymaterialization.TemporaryPrefix) {
			t.Fatalf("plaintext temporary remains: %q", entry.Name())
		}
	}
}

func assertMetadata(t *testing.T, name string, uid, gid, mode uint32, links uint64) {
	t.Helper()
	info, err := os.Lstat(name)
	if err != nil {
		t.Fatalf("inspect %s: %v", name, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("inspect %s: missing Linux metadata", name)
	}
	if stat.Uid != uid || stat.Gid != gid || uint32(info.Mode().Perm()) != mode {
		t.Fatalf(
			"metadata = %d:%d %04o, want %d:%d %04o",
			stat.Uid,
			stat.Gid,
			info.Mode().Perm(),
			uid,
			gid,
			mode,
		)
	}
	if links != 0 && stat.Nlink != links {
		t.Fatalf("link count = %d, want %d", stat.Nlink, links)
	}
}

type cancelingReader struct {
	reader io.Reader
	cancel context.CancelFunc
}

type armingCancelReader struct {
	reader io.Reader
	arm    func()
	done   bool
}

func (reader *armingCancelReader) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	if count > 0 && !reader.done {
		reader.done = true
		reader.arm()
	}
	return count, err
}

type trackingHasher struct {
	inner     entrymaterialization.Hasher
	destroyed bool
	verified  bool
}

func (hasher *trackingHasher) Write(content []byte) (int, error) {
	return hasher.inner.Write(content)
}

func (hasher *trackingHasher) Verify(expected entrymaterialization.Digest) bool {
	hasher.verified = true
	verified := hasher.inner.Verify(expected)
	hasher.destroyed = true
	return verified
}

func (hasher *trackingHasher) Destroy() {
	hasher.inner.Destroy()
	hasher.destroyed = true
}

func (reader *cancelingReader) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	if count > 0 {
		reader.cancel()
	}
	return count, err
}

type failingReader struct{ err error }

func (reader *failingReader) Read([]byte) (int, error) { return 0, reader.err }

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) { panic("nil context reached plaintext reader") }

type trackingReadCloser struct {
	io.Reader
	read   bool
	closed bool
}

func (source *trackingReadCloser) Read(target []byte) (int, error) {
	source.read = true
	if source.Reader == nil {
		panic("frame source read before nil-context rejection")
	}
	return source.Reader.Read(target)
}

func (source *trackingReadCloser) Close() error {
	source.closed = true
	return nil
}

type closeFailingReader struct{ io.Reader }

func (*closeFailingReader) Close() error { return errors.New("close blocked") }

func isInternal(err error) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindInternal
}
