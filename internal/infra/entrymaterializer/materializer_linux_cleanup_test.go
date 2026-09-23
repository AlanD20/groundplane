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
)

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
