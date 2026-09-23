//go:build linux

package backupstage

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// Rationale: a real second process must not overcommit the first process's
// remaining reservation after the first has also allocated staged bytes.
func TestMultiprocessReservationAndWriteRejectOvercommit(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	available, err := availableBytes(context.Background(), stager.rootFD, stager.ops)
	if err != nil {
		t.Fatalf("availableBytes() error = %v", err)
	}
	const safety = uint64(16 << 20)
	if available <= 3*safety || available > math.MaxInt64 {
		t.Skipf("filesystem availability %d cannot support bounded overcommit fixture", available)
	}
	stage, err := stager.Prepare(
		context.Background(), IDs{Task: "task-a", Step: "step", Point: "point"}, Bounded(
			available-safety))

	if err != nil {
		t.Fatalf("first Prepare() error = %v", err)
	}
	artifact, err := stage.CreateFile(context.Background(), "artifact.bin")
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := artifact.Write(context.Background(), make([]byte, 1<<20)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	result, err := runner.New(nil).Run(context.Background(), runner.RunCmdOpts{
		Name: executable, Args: []string{"-test.run=^TestLeaseProcessHelper$"},
		Env: []string{
			leaseHelperModeEnv + "=storage-conflict", leaseHelperRootEnv + "=" + root,
			leaseHelperTaskEnv + "=task-b", leaseHelperSizeEnv + "=" + strconv.FormatUint(2*safety, 10),
		},
	})
	if err != nil {
		t.Fatalf("overcommit helper error = %v, stdout = %s, stderr = %s", err, result.Stdout, result.Stderr)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

func runLeaseHelper(t *testing.T, root, mode string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	result, err := runner.New(nil).Run(context.Background(), runner.RunCmdOpts{
		Name: executable, Args: []string{"-test.run=^TestLeaseProcessHelper$"},
		Env: []string{leaseHelperModeEnv + "=" + mode, leaseHelperRootEnv + "=" + root},
	})
	if err != nil {
		t.Fatalf("lease helper error = %v, stdout = %s, stderr = %s", err, result.Stdout, result.Stderr)
	}
}

// Rationale: an unlocked reservation descriptor is authoritative evidence that
// its owner is gone. Recovery must not trust or validate abandoned bytes or
// mode metadata before unlinking the record and making that unlink durable.
func TestUnlockedMalformedReservationsAreReapedBeforeValidation(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	reservationDir := filepath.Join(root, reservationDirName)
	if err := os.Mkdir(reservationDir, os.FileMode(directoryMode)); err != nil {
		t.Fatalf("create reservation directory: %v", err)
	}
	fixtures := map[string]struct {
		contents []byte
		mode     os.FileMode
	}{
		"partial":    {contents: []byte{0, 1, 2}, mode: os.FileMode(fileMode)},
		"wrong-mode": {contents: make([]byte, reservationValueLen), mode: 0o640},
		"zero":       {contents: nil, mode: os.FileMode(fileMode)},
	}
	for name, fixture := range fixtures {
		stepDir := filepath.Join(reservationDir, name, "step")
		if err := os.MkdirAll(stepDir, os.FileMode(directoryMode)); err != nil {
			t.Fatalf("create %s reservation hierarchy: %v", name, err)
		}
		if err := os.Chmod(filepath.Join(reservationDir, name), os.FileMode(directoryMode)); err != nil {
			t.Fatalf("chmod %s reservation task: %v", name, err)
		}
		if err := os.Chmod(stepDir, os.FileMode(directoryMode)); err != nil {
			t.Fatalf("chmod %s reservation step: %v", name, err)
		}
		path := filepath.Join(stepDir, "point")
		if err := os.WriteFile(path, fixture.contents, fixture.mode); err != nil {
			t.Fatalf("write %s reservation: %v", name, err)
		}
		if err := os.Chmod(path, fixture.mode); err != nil {
			t.Fatalf("chmod %s reservation: %v", name, err)
		}
	}

	stager := newRootTestStager(t, root)
	stage, err := stager.Prepare(
		context.Background(), IDs{Task: "fresh-task", Step: "fresh-step", Point: "fresh-point"}, Bounded(
			1))

	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	for name := range fixtures {
		if _, err := os.Stat(filepath.Join(reservationDir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("stale reservation %q stat error = %v, want absent", name, err)
		}
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: the reservation hierarchy is a trusted namespace index. Invalid
// components, symlinks, non-file leaves, and lookup races must fail startup
// closed rather than redirecting or guessing abandoned-stage cleanup.
func TestRecoveryRejectsAmbiguousReservationNamespace(t *testing.T) {
	requireRoot(t)
	for _, test := range []struct {
		name  string
		setup func(*testing.T, string, *linuxOperations)
	}{
		{
			name: "reserved task name",
			setup: func(t *testing.T, root string, _ *linuxOperations) {
				reservationDir := createReservationRootFixture(t, root)
				if err := os.Mkdir(
					filepath.Join(reservationDir, ownerMarkerName), os.FileMode(directoryMode),
				); err != nil {
					t.Fatalf("create reserved reservation task: %v", err)
				}
			},
		},
		{
			name: "task symlink",
			setup: func(t *testing.T, root string, _ *linuxOperations) {
				reservationDir := createReservationRootFixture(t, root)
				target := filepath.Join(root, "target")
				if err := os.Mkdir(target, os.FileMode(directoryMode)); err != nil {
					t.Fatalf("create symlink target: %v", err)
				}
				if err := os.Symlink(target, filepath.Join(reservationDir, "task")); err != nil {
					t.Fatalf("create reservation task symlink: %v", err)
				}
			},
		},
		{
			name: "directory leaf",
			setup: func(t *testing.T, root string, _ *linuxOperations) {
				stepDir := createReservationHierarchyFixture(t, root, IDs{Task: "task", Step: "step"})
				if err := os.Mkdir(filepath.Join(stepDir, "point"), os.FileMode(directoryMode)); err != nil {
					t.Fatalf("create reservation directory leaf: %v", err)
				}
			},
		},
		{
			name: "leaf lookup race",
			setup: func(t *testing.T, root string, ops *linuxOperations) {
				stepDir := createReservationHierarchyFixture(t, root, IDs{Task: "task", Step: "step"})
				if err := os.WriteFile(filepath.Join(stepDir, "point"), nil, os.FileMode(fileMode)); err != nil {
					t.Fatalf("create raced reservation leaf: %v", err)
				}
				openat2 := ops.openat2
				raced := false
				ops.openat2 = func(dirFD int, path string, how *unix.OpenHow) (int, error) {
					if !raced && path == "point" && how.Flags&uint64(unix.O_CREAT) == 0 {
						raced = true
						if err := unix.Unlinkat(dirFD, path, 0); err != nil {
							return -1, err
						}
					}
					return openat2(dirFD, path, how)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := privateRootTestFixture(t)
			ops := defaultLinuxOperations()
			test.setup(t, root, &ops)
			stager, err := openRecoveryWithOperations(context.Background(), Config{Root: root}, ops)
			if stager != nil {
				if closeErr := stager.Close(context.Background()); closeErr != nil {
					t.Errorf("unexpected Stager.Close() error = %v", closeErr)
				}
			}
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("OpenRecovery() error = %v, want internal", err)
			}
		})
	}
}

// Rationale: startup recovery is the singleton ownership barrier; observing a
// live reservation means another Agent owns this root and construction fails.
func TestRecoveryRejectsLiveReservation(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	first := newRootTestStager(t, root)
	stage, err := first.Prepare(
		context.Background(), IDs{Task: "task", Step: "step", Point: "point"}, Bounded(
			1))

	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	second, err := OpenRecovery(context.Background(), Config{Root: root})
	if second != nil {
		if closeErr := second.Close(context.Background()); closeErr != nil {
			t.Errorf("unexpected second Stager.Close() error = %v", closeErr)
		}
	}
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("second OpenRecovery() error = %v, want state conflict", err)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

func createReservationRootFixture(t *testing.T, root string) string {
	t.Helper()
	reservationDir := filepath.Join(root, reservationDirName)
	if err := os.Mkdir(reservationDir, os.FileMode(directoryMode)); err != nil {
		t.Fatalf("create reservation root: %v", err)
	}
	return reservationDir
}

func createReservationHierarchyFixture(t *testing.T, root string, ids IDs) string {
	t.Helper()
	reservationDir := createReservationRootFixture(t, root)
	stepDir := filepath.Join(reservationDir, ids.Task, ids.Step)
	if err := os.MkdirAll(stepDir, os.FileMode(directoryMode)); err != nil {
		t.Fatalf("create reservation hierarchy: %v", err)
	}
	if err := os.Chmod(filepath.Join(reservationDir, ids.Task), os.FileMode(directoryMode)); err != nil {
		t.Fatalf("chmod reservation task: %v", err)
	}
	if err := os.Chmod(stepDir, os.FileMode(directoryMode)); err != nil {
		t.Fatalf("chmod reservation step: %v", err)
	}
	return stepDir
}

// Rationale: locked reservation records serialize Stager instances, reject
// aggregate overcommit, and release all accounting on Close.
func TestConcurrentReservationAccountingAndRelease(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	ops := defaultLinuxOperations()
	ops.fstatfs = func(_ int, stat *unix.Statfs_t) error {
		stat.Bsize, stat.Bavail = 1, 100
		return nil
	}
	firstStager := newRootTestStagerWithOperations(t, root, ops)
	secondStager := firstStager
	first, err := firstStager.Prepare(
		context.Background(), IDs{Task: "task-a", Step: "step", Point: "point"}, Bounded(
			70))

	if err != nil {
		t.Fatalf("first Prepare() error = %v", err)
	}
	if _, err := secondStager.Prepare(
		context.Background(), IDs{Task: "task-b", Step: "step", Point: "point"}, Bounded(
			40)); !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
		t.Fatalf("overcommitted Prepare() error = %v, want storage unavailable", err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	second, err := secondStager.Prepare(
		context.Background(), IDs{Task: "task-b", Step: "step", Point: "point"}, Bounded(
			40))

	if err != nil {
		t.Fatalf("Prepare() after release error = %v", err)
	}
	if err := second.Cleanup(context.Background()); err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
}

// Rationale: a failed reservation journal append must become visibly corrupt
// before releasing the root lock, so concurrent capacity admission fails
// closed until the poisoned stage is cleaned.
func TestReservationUpdateFailurePoisonsConcurrentAdmission(t *testing.T) {
	requireRoot(t)
	for _, failure := range []string{"short-pwrite", "fdatasync"} {
		t.Run(failure, func(t *testing.T) {
			root := privateRootTestFixture(t)
			firstStager := newRootTestStager(t, root)
			secondStager := firstStager
			stage, err := firstStager.Prepare(
				context.Background(), IDs{Task: "first", Step: "step", Point: "point"}, Bounded(100),
			)
			if err != nil {
				t.Fatalf("Prepare() error = %v", err)
			}
			artifact, err := stage.CreateFile(context.Background(), "artifact.bin")
			if err != nil {
				t.Fatalf("CreateFile() error = %v", err)
			}
			updateStarted := make(chan struct{})
			allowFailure := make(chan struct{})
			if failure == "short-pwrite" {
				pwrite := firstStager.ops.pwrite
				calls := 0
				firstStager.ops.pwrite = func(fd int, content []byte, offset int64) (int, error) {
					calls++
					if calls == 1 {
						written, writeErr := pwrite(fd, content[:len(content)/2], offset)
						close(updateStarted)
						<-allowFailure
						return written, writeErr
					}
					return 0, unix.EIO
				}
			} else {
				calls := 0
				firstStager.ops.fdatasync = func(int) error {
					calls++
					if calls == 1 {
						close(updateStarted)
						<-allowFailure
					}
					return unix.EIO
				}
			}
			writeErr := make(chan error, 1)
			go func() {
				_, err := artifact.Write(context.Background(), []byte("bytes"))
				writeErr <- err
			}()
			<-updateStarted
			prepareErr := make(chan error, 1)
			go func() {
				_, err := secondStager.Prepare(
					context.Background(), IDs{Task: "second", Step: "step", Point: "point"}, Bounded(1),
				)
				prepareErr <- err
			}()
			select {
			case err := <-prepareErr:
				t.Fatalf("concurrent Prepare() returned before reservation update completed: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			close(allowFailure)
			if err := <-writeErr; err == nil {
				t.Fatal("Write() succeeded")
			}
			if !stage.poisoned {
				t.Fatal("stage was not poisoned")
			}
			if err := <-prepareErr; !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("concurrent Prepare() error = %v, want internal", err)
			}
			firstStager.ops.pwrite = defaultLinuxOperations().pwrite
			firstStager.ops.fdatasync = defaultLinuxOperations().fdatasync
			if err := stage.Cleanup(context.Background()); err != nil {
				t.Fatalf("Cleanup() error = %v", err)
			}
			recovered, err := secondStager.Prepare(
				context.Background(), IDs{Task: "second", Step: "step", Point: "point"}, Bounded(1),
			)
			if err != nil {
				t.Fatalf("Prepare() after poisoned cleanup error = %v", err)
			}
			if err := recovered.Cleanup(context.Background()); err != nil {
				t.Fatalf("recovered Cleanup() error = %v", err)
			}
		})
	}
}

// Rationale: the filesystem growth marker is the cross-process admission
// authority. Bounded stages retain a shared lease, unknown stages retain an
// exclusive lease, and an unknown stage releases it only after every growing
// artifact has been durably sealed.
func TestBoundedAndExclusiveUnknownAdmission(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	unknownStager := newRootTestStager(t, root)
	boundedStager := unknownStager
	otherUnknownStager := unknownStager
	unknown, err := unknownStager.Prepare(
		context.Background(), IDs{Task: "unknown", Step: "step", Point: "point"}, ExclusiveUnknown(),
	)
	if err != nil {
		t.Fatalf("unknown Prepare() error = %v", err)
	}
	if _, err := boundedStager.Prepare(
		context.Background(), IDs{Task: "bounded", Step: "step", Point: "point"}, Bounded(1),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("bounded Prepare() during unknown growth error = %v, want state conflict", err)
	}
	if _, err := otherUnknownStager.Prepare(
		context.Background(), IDs{Task: "other-unknown", Step: "step", Point: "point"}, ExclusiveUnknown(),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("second unknown Prepare() error = %v, want state conflict", err)
	}
	first, err := unknown.CreateFile(context.Background(), "first.bin")
	if err != nil {
		t.Fatalf("first CreateFile() error = %v", err)
	}
	second, err := unknown.CreateFile(context.Background(), "second.bin")
	if err != nil {
		t.Fatalf("second CreateFile() error = %v", err)
	}
	if _, err := first.Write(context.Background(), []byte("first")); err != nil {
		t.Fatalf("first Write() error = %v", err)
	}
	if _, err := second.Write(context.Background(), []byte("second")); err != nil {
		t.Fatalf("second Write() error = %v", err)
	}
	if _, err := first.Publish(context.Background()); err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}
	if _, err := boundedStager.Prepare(
		context.Background(), IDs{Task: "bounded", Step: "step", Point: "point"}, Bounded(1),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("bounded Prepare() before all seals error = %v, want state conflict", err)
	}
	if _, err := second.Publish(context.Background()); err != nil {
		t.Fatalf("second Publish() error = %v", err)
	}
	bounded, err := boundedStager.Prepare(
		context.Background(), IDs{Task: "bounded", Step: "step", Point: "point"}, Bounded(1),
	)
	if err != nil {
		t.Fatalf("bounded Prepare() after all seals error = %v", err)
	}
	if err := bounded.Cleanup(context.Background()); err != nil {
		t.Fatalf("bounded Cleanup() error = %v", err)
	}
	if err := unknown.Cleanup(context.Background()); err != nil {
		t.Fatalf("unknown Cleanup() error = %v", err)
	}
}

// Rationale: a live bounded reservation's shared growth lease prevents an
// unknown-size producer from bypassing deterministic capacity accounting.
func TestBoundedAdmissionExcludesUnknownGrowth(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	boundedStager := newRootTestStager(t, root)
	unknownStager := boundedStager
	bounded, err := boundedStager.Prepare(
		context.Background(), IDs{Task: "bounded", Step: "step", Point: "point"}, Bounded(1),
	)
	if err != nil {
		t.Fatalf("bounded Prepare() error = %v", err)
	}
	if _, err := unknownStager.Prepare(
		context.Background(), IDs{Task: "unknown", Step: "step", Point: "point"}, ExclusiveUnknown(),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("unknown Prepare() during bounded stage error = %v, want state conflict", err)
	}
	if err := bounded.Cleanup(context.Background()); err != nil {
		t.Fatalf("bounded Cleanup() error = %v", err)
	}
	unknown, err := unknownStager.Prepare(
		context.Background(), IDs{Task: "unknown", Step: "step", Point: "point"}, ExclusiveUnknown(),
	)
	if err != nil {
		t.Fatalf("unknown Prepare() after bounded cleanup error = %v", err)
	}
	if err := unknown.Cleanup(context.Background()); err != nil {
		t.Fatalf("unknown Cleanup() error = %v", err)
	}
}

// Rationale: unsupported KEEP_SIZE allocation must fail during the mandatory
// construction barrier, before any unknown-size producer can be launched.
func TestRecoveryProbesKeepSizeFallocate(t *testing.T) {
	requireRoot(t)
	ops := defaultLinuxOperations()
	ops.fallocate = func(int, uint32, int64, int64) error { return unix.EOPNOTSUPP }
	stager, err := openRecoveryWithOperations(context.Background(), Config{Root: privateRootTestFixture(t)}, ops)
	if stager != nil {
		if closeErr := stager.Close(context.Background()); closeErr != nil {
			t.Errorf("unexpected Stager.Close() error = %v", closeErr)
		}
	}
	if !errors.Is(err, errs.New(errs.KindNotImplemented, "")) {
		t.Fatalf("OpenRecovery() error = %v, want not implemented", err)
	}
}

// Rationale: every unknown-size write allocates exactly its unwritten logical
// range before writing, and publication seals the counted descriptor size.
func TestExclusiveUnknownAllocatesExactRangesAndSeals(t *testing.T) {
	requireRoot(t)
	stager := newRootTestStager(t, privateRootTestFixture(t))
	stage, err := stager.Prepare(
		context.Background(), IDs{Task: "task", Step: "step", Point: "point"}, ExclusiveUnknown(),
	)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	artifact, err := stage.CreateFile(context.Background(), "artifact.bin")
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	type allocation struct{ offset, length int64 }
	var allocations []allocation
	stager.ops.fallocate = func(fd int, mode uint32, offset, length int64) error {
		allocations = append(allocations, allocation{offset: offset, length: length})
		return unix.Fallocate(fd, mode, offset, length)
	}
	if _, err := artifact.Write(context.Background(), []byte("abc")); err != nil {
		t.Fatalf("first Write() error = %v", err)
	}
	if _, err := artifact.Write(context.Background(), []byte("de")); err != nil {
		t.Fatalf("second Write() error = %v", err)
	}
	if len(allocations) != 2 || allocations[0] != (allocation{offset: 0, length: 3}) ||
		allocations[1] != (allocation{offset: 3, length: 2}) {
		t.Fatalf("allocations = %#v, want [{0 3} {3 2}]", allocations)
	}
	if _, err := artifact.Publish(context.Background()); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if artifact.written != 5 || artifact.size != 5 {
		t.Fatalf("sealed counts = written %d size %d, want 5", artifact.written, artifact.size)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: allocation failure, write failure, cancellation after allocation,
// fsync failure, and descriptor-size mismatch all remove the partial and end
// exclusive growth rather than leaving reusable or unaccounted bytes.
func TestExclusiveUnknownFailuresCleanPartial(t *testing.T) {
	requireRoot(t)
	for _, failure := range []string{"allocation", "write", "cancel", "cancel-after-write", "fsync", "size"} {
		t.Run(failure, func(t *testing.T) {
			root := privateRootTestFixture(t)
			stager := newRootTestStager(t, root)
			boundedStager := stager
			stage, err := stager.Prepare(
				context.Background(), IDs{Task: "unknown", Step: "step", Point: "point"}, ExclusiveUnknown(),
			)
			if err != nil {
				t.Fatalf("Prepare() error = %v", err)
			}
			artifact, err := stage.CreateFile(context.Background(), "artifact.bin")
			if err != nil {
				t.Fatalf("CreateFile() error = %v", err)
			}
			partial := filepath.Join(root, "unknown", "step", "point", ".artifact.bin.partial")
			ctx := context.Background()
			switch failure {
			case "allocation":
				stager.ops.fallocate = func(int, uint32, int64, int64) error { return unix.ENOSPC }
			case "write":
				stager.ops.write = func(int, []byte) (int, error) { return 0, unix.EIO }
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(context.Background())
				stager.ops.fallocate = func(int, uint32, int64, int64) error {
					cancel()
					return nil
				}
			case "cancel-after-write":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(context.Background())
				write := stager.ops.write
				stager.ops.write = func(fd int, content []byte) (int, error) {
					written, writeErr := write(fd, content)
					cancel()
					return written, writeErr
				}
			}
			if failure == "fsync" || failure == "size" {
				if _, err := artifact.Write(ctx, []byte("bytes")); err != nil {
					t.Fatalf("Write() before %s error = %v", failure, err)
				}
				if failure == "fsync" {
					artifactFD := int(artifact.file.Fd())
					fsync := stager.ops.fsync
					stager.ops.fsync = func(fd int) error {
						if fd == artifactFD {
							return unix.EIO
						}
						return fsync(fd)
					}
				} else if err := unix.Ftruncate(int(artifact.file.Fd()), 1); err != nil {
					t.Fatalf("truncate size-mismatch fixture: %v", err)
				}
				_, err = artifact.Publish(ctx)
			} else {
				_, err = artifact.Write(ctx, []byte("bytes"))
			}
			if err == nil {
				t.Fatalf("%s operation succeeded", failure)
			}
			if failure == "allocation" && !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
				t.Fatalf("allocation error = %v, want storage unavailable", err)
			}
			if (failure == "cancel" || failure == "cancel-after-write") &&
				!errors.Is(err, context.Canceled) {
				t.Fatalf("cancel error = %v, want context canceled", err)
			}
			if _, statErr := os.Stat(partial); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("partial stat error = %v, want absent", statErr)
			}
			bounded, prepareErr := boundedStager.Prepare(
				context.Background(), IDs{Task: "bounded", Step: "step", Point: "point"}, Bounded(1),
			)
			if prepareErr != nil {
				t.Fatalf("bounded Prepare() after failure error = %v", prepareErr)
			}
			if err := bounded.Cleanup(context.Background()); err != nil {
				t.Fatalf("bounded Cleanup() error = %v", err)
			}
			if err := stage.Cleanup(context.Background()); err != nil {
				t.Fatalf("unknown Cleanup() error = %v", err)
			}
		})
	}
}

// Rationale: metadata on managed artifact descriptors is private state, not
// caller input. Create, Publish, and Open must classify tampering as Internal.
func TestManagedArtifactMetadataMismatchIsInternal(t *testing.T) {
	requireRoot(t)
	for _, operation := range []string{"create", "publish", "open"} {
		t.Run(operation, func(t *testing.T) {
			root := privateRootTestFixture(t)
			stager := newRootTestStager(t, root)
			stage, err := stager.Prepare(
				context.Background(), IDs{Task: "task", Step: "step", Point: "point"}, Bounded(16),
			)
			if err != nil {
				t.Fatalf("Prepare() error = %v", err)
			}
			if operation == "create" {
				openat2 := stager.ops.openat2
				stager.ops.openat2 = func(dirFD int, path string, how *unix.OpenHow) (int, error) {
					fd, openErr := openat2(dirFD, path, how)
					if openErr == nil && path == ".artifact.bin.partial" {
						if linkErr := unix.Linkat(dirFD, path, dirFD, ".extra-link", 0); linkErr != nil {
							return -1, linkErr
						}
					}
					return fd, openErr
				}
				_, err = stage.CreateFile(context.Background(), "artifact.bin")
				if !errors.Is(err, errs.New(errs.KindInternal, "")) {
					t.Fatalf("CreateFile() error = %v, want internal", err)
				}
				if unlinkErr := os.Remove(
					filepath.Join(root, "task", "step", "point", ".extra-link"),
				); unlinkErr != nil {
					t.Fatalf("remove extra link fixture: %v", unlinkErr)
				}
			} else {
				artifact, createErr := stage.CreateFile(context.Background(), "artifact.bin")
				if createErr != nil {
					t.Fatalf("CreateFile() error = %v", createErr)
				}
				if _, writeErr := artifact.Write(context.Background(), []byte("bytes")); writeErr != nil {
					t.Fatalf("Write() error = %v", writeErr)
				}
				if operation == "publish" {
					if chmodErr := unix.Fchmod(int(artifact.file.Fd()), 0o640); chmodErr != nil {
						t.Fatalf("chmod publish fixture: %v", chmodErr)
					}
					_, err = artifact.Publish(context.Background())
					if !errors.Is(err, errs.New(errs.KindInternal, "")) {
						t.Fatalf("Publish() error = %v, want internal", err)
					}
				} else {
					if _, publishErr := artifact.Publish(context.Background()); publishErr != nil {
						t.Fatalf("Publish() error = %v", publishErr)
					}
					if chmodErr := unix.Fchmod(int(artifact.file.Fd()), 0o640); chmodErr != nil {
						t.Fatalf("chmod open fixture: %v", chmodErr)
					}
					_, err = artifact.Open(context.Background())
					if !errors.Is(err, errs.New(errs.KindInternal, "")) {
						t.Fatalf("Open() error = %v, want internal", err)
					}
					if chmodErr := unix.Fchmod(int(artifact.file.Fd()), fileMode); chmodErr != nil {
						t.Fatalf("restore open fixture mode: %v", chmodErr)
					}
				}
			}
			if err := stage.Cleanup(context.Background()); err != nil {
				t.Fatalf("Cleanup() error = %v", err)
			}
		})
	}
}
