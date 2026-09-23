//go:build linux

package backupstage

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// Rationale: fstatfs multiplication must fail on overflow rather than wrap to
// an apparently sufficient byte count.
func TestReservationRejectsFstatfsOverflow(t *testing.T) {
	requireRoot(t)
	ops := defaultLinuxOperations()
	ops.fstatfs = func(_ int, stat *unix.Statfs_t) error {
		stat.Bsize, stat.Bavail = 2, math.MaxUint64
		return nil
	}
	stager := newRootTestStagerWithOperations(t, t.TempDir(), ops)
	if _, err := stager.Prepare(
		context.Background(), IDs{Task: "task", Step: "step", Point: "point"}, Bounded(
			1)); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Prepare() error = %v, want internal overflow failure", err)
	}
}

// Rationale: readers must remain on the retained inode after the final name is
// replaced, closing the pathname-reopen TOCTOU.
func TestPublishedReaderDoesNotReopenReplacedName(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	stage, artifact, directory := prepareRootArtifact(t, stager, root)
	if _, err := artifact.Publish(context.Background()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	final := filepath.Join(directory, "artifact.bin")
	if err := os.Rename(final, final+".moved"); err != nil {
		t.Fatalf("move published name: %v", err)
	}
	if err := os.WriteFile(final, []byte("replacement"), 0o600); err != nil {
		t.Fatalf("write replacement: %v", err)
	}
	reader, err := artifact.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	content, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || string(content) != "plaintext" {
		t.Fatalf("content = %q, read error = %v, close error = %v", content, readErr, closeErr)
	}
	if err := os.Remove(final); err != nil {
		t.Fatalf("remove replacement: %v", err)
	}
	if err := os.Rename(final+".moved", final); err != nil {
		t.Fatalf("restore published name: %v", err)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: staging always uses one named O_EXCL partial and retains its
// original O_RDWR descriptor through descriptor-relative publication.
func TestNamedPartialRetainsReadWriteFD(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	stage, artifact, directory := prepareRootArtifact(t, stager, root)
	flags, err := unix.FcntlInt(artifact.file.Fd(), unix.F_GETFL, 0)
	if err != nil {
		t.Fatalf("F_GETFL error = %v", err)
	}
	if flags&unix.O_ACCMODE != unix.O_RDWR {
		t.Fatalf("partial flags = %#x, want O_RDWR", flags)
	}
	if _, err := os.Stat(filepath.Join(directory, ".artifact.bin.partial")); err != nil {
		t.Fatalf("named partial stat error = %v", err)
	}
	if _, err := artifact.Publish(context.Background()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if artifact.file == nil {
		t.Fatal("Publish() closed the original descriptor")
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: replacement of the configured pathname must not retarget the
// already anchored root inode.
func TestConfiguredRootInodeReplacementDoesNotRetargetStager(t *testing.T) {
	requireRoot(t)
	parent := t.TempDir()
	root, anchored := filepath.Join(parent, "stage"), filepath.Join(parent, "anchored")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("create stage root: %v", err)
	}
	stager := newRootTestStager(t, root)
	if err := os.Rename(root, anchored); err != nil {
		t.Fatalf("rename configured root: %v", err)
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("create replacement root: %v", err)
	}
	stage, err := stager.Prepare(
		context.Background(), IDs{Task: "task", Step: "step", Point: "point"}, Bounded(
			1024))

	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(anchored, "task")); err != nil {
		t.Fatalf("anchored task stat error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "task")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement task stat error = %v, want absent", err)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: task cancellation must be reported only after Close has removed
// names and released the lease and capacity reservation.
func TestCanceledCloseStillPerformsMandatoryCleanup(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	ids := IDs{Task: "task", Step: "step", Point: "point"}
	stage, err := stager.Prepare(context.Background(), ids, Bounded(1024))
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	artifact, err := stage.CreateFile(context.Background(), "artifact.bin")
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := artifact.Write(context.Background(), []byte("bytes")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := stage.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close() error = %v, want cancellation after cleanup", err)
	}
	if _, err := os.Stat(filepath.Join(root, ids.Task)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("task stat error = %v, want absent", err)
	}
	recovered, err := stager.Prepare(context.Background(), ids, Bounded(1024))
	if err != nil {
		t.Fatalf("Prepare() after canceled Close error = %v", err)
	}
	if err := recovered.Cleanup(context.Background()); err != nil {
		t.Fatalf("recovered Cleanup() error = %v", err)
	}
}

// Rationale: direct Cleanup and artifact Abort must also ignore task
// cancellation until names, leases, and reservations have been released.
func TestCanceledCleanupAndAbortStillReleaseStage(t *testing.T) {
	requireRoot(t)
	for _, action := range []string{"cleanup", "abort"} {
		t.Run(action, func(t *testing.T) {
			root := privateRootTestFixture(t)
			stager := newRootTestStager(t, root)
			ids := IDs{Task: "task", Step: "step", Point: "point"}
			stage, err := stager.Prepare(context.Background(), ids, Bounded(1024))
			if err != nil {
				t.Fatalf("Prepare() error = %v", err)
			}
			artifact, err := stage.CreateFile(context.Background(), "artifact.bin")
			if err != nil {
				t.Fatalf("CreateFile() error = %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if action == "cleanup" {
				err = stage.Cleanup(ctx)
			} else {
				err = artifact.Abort(ctx)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("%s error = %v, want cancellation after cleanup", action, err)
			}
			if _, err := os.Stat(filepath.Join(root, ids.Task)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("task stat error = %v, want absent", err)
			}
			recovered, err := stager.Prepare(context.Background(), ids, Bounded(1024))
			if err != nil {
				t.Fatalf("Prepare() after canceled %s error = %v", action, err)
			}
			if err := recovered.Cleanup(context.Background()); err != nil {
				t.Fatalf("recovered Cleanup() error = %v", err)
			}
		})
	}
}

// Rationale: expiry of the bounded independent cleanup context is Internal,
// but descriptors and locks still release so a fresh Stager can reap evidence.
func TestCleanupTimeoutIsInternalAndReleasesKernelResources(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	ids := IDs{Task: "task", Step: "step", Point: "point"}
	stage, err := stager.Prepare(context.Background(), ids, Bounded(1024))
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	flock := stager.ops.flock
	stager.ops.cleanupTimeout = time.Millisecond
	stager.ops.flock = func(fd, operation int) error {
		if operation == unix.LOCK_EX|unix.LOCK_NB {
			return unix.EWOULDBLOCK
		}
		return flock(fd, operation)
	}
	err = stage.Cleanup(context.Background())
	kind, ok := errs.KindOf(err)
	if !ok || kind != errs.KindInternal {
		t.Fatalf("Cleanup() error = %v, kind = %v/%t, want Internal", err, kind, ok)
	}
	if err := stager.Close(context.Background()); err != nil {
		t.Fatalf("Stager.Close() after cleanup timeout error = %v", err)
	}
	recovery := newRootTestStager(t, root)
	recovered, err := recovery.Prepare(context.Background(), ids, Bounded(1024))
	if err != nil {
		t.Fatalf("Prepare() after cleanup timeout error = %v", err)
	}
	if err := recovered.Cleanup(context.Background()); err != nil {
		t.Fatalf("recovered Cleanup() error = %v", err)
	}
}

// Rationale: ENOSPC and EDQUOT at artifact create, write, and fsync boundaries
// are retryable storage failures rather than opaque internal errors.
func TestArtifactStoragePressureMapsToStorageUnavailable(t *testing.T) {
	requireRoot(t)
	for _, test := range []struct {
		name   string
		mutate func(*linuxOperations)
		run    func(context.Context, *Stage) error
	}{
		{
			name: "create ENOSPC",
			mutate: func(ops *linuxOperations) {
				openat2 := ops.openat2
				ops.openat2 = func(fd int, name string, how *unix.OpenHow) (int, error) {
					if name == ".artifact.bin.partial" {
						return -1, unix.ENOSPC
					}
					return openat2(fd, name, how)
				}
			},
			run: func(ctx context.Context, stage *Stage) error {
				_, err := stage.CreateFile(ctx, "artifact.bin")
				return err
			},
		},
		{
			name: "write ENOSPC",
			mutate: func(ops *linuxOperations) {
				ops.write = func(int, []byte) (int, error) { return 0, unix.ENOSPC }
			},
			run: func(ctx context.Context, stage *Stage) error {
				artifact, err := stage.CreateFile(ctx, "artifact.bin")
				if err != nil {
					return err
				}
				_, err = artifact.Write(ctx, []byte("bytes"))
				return err
			},
		},
		{
			name: "fsync EDQUOT",
			mutate: func(ops *linuxOperations) {
				ops.fsync = func(int) error { return unix.EDQUOT }
			},
			run: func(ctx context.Context, stage *Stage) error {
				artifact, err := stage.CreateFile(ctx, "artifact.bin")
				if err != nil {
					return err
				}
				_, err = artifact.Publish(ctx)
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := privateRootTestFixture(t)
			ops := defaultLinuxOperations()
			stager := newRootTestStagerWithOperations(t, root, ops)
			stage, err := stager.Prepare(
				context.Background(), IDs{Task: "task", Step: "step", Point: "point"}, Bounded(
					1024))

			if err != nil {
				t.Fatalf("Prepare() error = %v", err)
			}
			test.mutate(&stager.ops)
			if err := test.run(context.Background(), stage); !errors.Is(
				err, errs.New(errs.KindStorageUnavailable, ""),
			) {
				t.Fatalf("operation error = %v, want storage unavailable", err)
			}
			if err := stage.Cleanup(context.Background()); err != nil {
				t.Fatalf("Cleanup() error = %v", err)
			}
		})
	}
}

// Rationale: a reader is bound to the context supplied to Open, including
// cancellation that occurs after descriptor duplication.
func TestPublishedReaderHonorsOpenContextCancellation(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	stage, artifact, _ := prepareRootArtifact(t, stager, root)
	if _, err := artifact.Publish(context.Background()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	reader, err := artifact.Open(ctx)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	cancel()
	if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read() error = %v, want cancellation", err)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: cleanup revokes every duplicate under the same lock order as
// reads, so a later fd reuse cannot make a stale reader observe another file.
func TestConcurrentCleanupRevokesReadersWithoutFDReuseRace(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	stage, artifact, _ := prepareRootArtifact(t, stager, root)
	if _, err := artifact.Publish(context.Background()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	first, err := artifact.Open(context.Background())
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	second, err := artifact.Open(context.Background())
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, err := first.Read(make([]byte, 1))
		results <- err
	}()
	go func() {
		<-start
		results <- stage.Cleanup(context.Background())
	}()
	close(start)
	for range 2 {
		if err := <-results; err != nil && !errors.Is(err, os.ErrClosed) {
			t.Fatalf("concurrent operation error = %v", err)
		}
	}
	reused, err := os.Open("/dev/null")
	if err != nil {
		t.Fatalf("open fd-reuse fixture: %v", err)
	}
	defer func() {
		if err := reused.Close(); err != nil {
			t.Errorf("close fd-reuse fixture: %v", err)
		}
	}()
	for _, reader := range []io.ReadCloser{first, second} {
		if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("revoked reader error = %v, want closed descriptor", err)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("revoked reader Close() error = %v", err)
		}
	}
}

func prepareRootArtifact(t *testing.T, stager *Stager, root string) (*Stage, *Artifact, string) {
	t.Helper()
	ids := IDs{Task: "task", Step: "step", Point: "point"}
	stage, err := stager.Prepare(context.Background(), ids, Bounded(1024))
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	artifact, err := stage.CreateFile(context.Background(), "artifact.bin")
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := artifact.Write(context.Background(), []byte("plaintext")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	return stage, artifact, filepath.Join(root, ids.Task, ids.Step, ids.Point)
}

func newRootTestStager(t *testing.T, root string) *Stager {
	t.Helper()
	return newRootTestStagerWithOperations(t, root, defaultLinuxOperations())
}

func newRootTestStagerWithOperations(t *testing.T, root string, ops linuxOperations) *Stager {
	t.Helper()
	normalizeRootTestFixture(t, root)
	session, err := openRecoveryWithOperations(context.Background(), Config{Root: root}, ops)
	if err != nil {
		t.Fatalf("OpenRecovery() error = %v", err)
	}
	for _, recovered := range session.Inventory() {
		if err := session.DiscardRecovered(context.Background(), recovered.RecoveryID, "test-discard"); err != nil {
			t.Fatalf("DiscardRecovered() error = %v", err)
		}
	}
	stager, prepared, err := session.Complete(context.Background())
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if len(prepared) != 0 {
		t.Fatalf("Complete() prepared %d stages, want none", len(prepared))
	}
	t.Cleanup(func() {
		if err := stager.Close(context.Background()); err != nil {
			t.Errorf("Stager.Close() error = %v", err)
		}
	})
	return stager
}

// Rationale: t.TempDir follows the process umask, while production staging
// intentionally requires the pre-created root to be root-owned with exact 0700.
func privateRootTestFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	normalizeRootTestFixture(t, root)
	return root
}

func normalizeRootTestFixture(t *testing.T, root string) {
	t.Helper()
	if err := os.Chown(root, 0, 0); err != nil {
		t.Fatalf("chown staging root fixture: %v", err)
	}
	if err := os.Chmod(root, os.FileMode(directoryMode)); err != nil {
		t.Fatalf("chmod staging root fixture: %v", err)
	}
}

func requireRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("root-only metadata success test")
	}
}

func assertPrivateMetadata(t *testing.T, path string, directory bool) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat(%q) error = %v", path, err)
	}
	wantMode := os.FileMode(0o600)
	if directory {
		wantMode = 0o700
		if !info.IsDir() {
			t.Fatalf("%q is not a directory", path)
		}
	} else if !info.Mode().IsRegular() {
		t.Fatalf("%q is not a regular file", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if info.Mode().Perm() != wantMode || !ok || stat.Uid != 0 || stat.Gid != 0 {
		t.Fatalf("%q has unsafe metadata: mode=%04o stat=%#v", path, info.Mode().Perm(), info.Sys())
	}
	if !directory && stat.Nlink != 1 {
		t.Fatalf("%q link count = %d, want 1", path, stat.Nlink)
	}
}
