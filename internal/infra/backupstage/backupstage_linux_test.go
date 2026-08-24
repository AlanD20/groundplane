//go:build linux

package backupstage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// Rationale: traversal and separator validation is a pure security boundary
// and must run in unprivileged CI rather than being hidden behind a root skip.
func TestValidateIDsRejectsTraversalAndUnsafeIdentifiers(t *testing.T) {
	for _, test := range []struct {
		name string
		ids  IDs
	}{
		{name: "task traversal", ids: IDs{Task: "../task", Step: "step", Point: "point"}},
		{name: "step traversal", ids: IDs{Task: "task", Step: "step/../other", Point: "point"}},
		{name: "point absolute", ids: IDs{Task: "task", Step: "step", Point: "/tmp/point"}},
		{name: "empty task", ids: IDs{Task: "", Step: "step", Point: "point"}},
		{name: "backslash", ids: IDs{Task: `task\escape`, Step: "step", Point: "point"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateIDs(test.ids); err == nil {
				t.Fatal("validateIDs() accepted an unsafe identifier")
			}
		})
	}
}

// Rationale: caller-controlled final names must never overlap recovery's
// managed partial grammar or any package-owned namespace marker.
func TestFinalArtifactNameRejectsEveryManagedSpelling(t *testing.T) {
	for _, name := range []string{
		reservationDirName,
		growthMarkerName,
		ownerMarkerName,
		".artifact.bin.partial",
		"..artifact.bin.partial",
		"...artifact.bin.partial.partial",
	} {
		if err := validateFinalArtifactName(name); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("validateFinalArtifactName(%q) error = %v, want validation", name, err)
		}
	}
	for _, name := range []string{"artifact.bin", "source.final", "stored.final"} {
		if err := validateFinalArtifactName(name); err != nil {
			t.Fatalf("validateFinalArtifactName(%q) error = %v", name, err)
		}
	}
}

// Rationale: Task is the first caller-controlled component below the staging
// root, so internal top-level and marker spellings must fail validation.
func TestTaskIDRejectsEveryManagedSpelling(t *testing.T) {
	for _, task := range []string{
		reservationDirName,
		growthMarkerName,
		ownerMarkerName,
		".task.partial",
		"..task.partial",
	} {
		err := validateIDs(IDs{Task: task, Step: "step", Point: "point"})
		if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("validateIDs(Task: %q) error = %v, want validation", task, err)
		}
	}
}

// Rationale: startup roots are configuration authorities, so aliases such as
// dot segments, trailing separators, and malformed UTF-8 must fail closed.
func TestValidateRootPathRejectsNonCanonicalValues(t *testing.T) {
	for _, root := range []string{"", "/", "relative", "/tmp/../stage", "/tmp/stage/", "/tmp/\xff"} {
		if _, err := validateRootPath(root); err == nil {
			t.Fatalf("validateRootPath(%q) accepted a non-canonical root", root)
		}
	}
}

// Rationale: the configured root may be a dedicated mount, while every path
// resolved after anchoring it must reject mount crossings.
func TestResolutionPoliciesSeparateConfiguredRootFromDescendants(t *testing.T) {
	if rootResolvePolicy&unix.RESOLVE_NO_XDEV != 0 {
		t.Fatal("configured-root policy unexpectedly rejects mount crossings")
	}
	if descendantResolvePolicy&unix.RESOLVE_NO_XDEV == 0 {
		t.Fatal("descendant policy does not reject mount crossings")
	}
	for _, required := range []uint64{
		unix.RESOLVE_BENEATH,
		unix.RESOLVE_NO_SYMLINKS,
		unix.RESOLVE_NO_MAGICLINKS,
	} {
		if rootResolvePolicy&required == 0 || descendantResolvePolicy&required == 0 {
			t.Fatalf("resolution policies omit required flag %#x", required)
		}
	}
}

// Rationale: unsupported kernels or seccomp profiles must fail during New
// with a clear baseline error, before accepting backup bytes.
func TestNewReportsMissingOpenat2AtStartup(t *testing.T) {
	ops := defaultLinuxOperations()
	ops.openat2 = func(int, string, *unix.OpenHow) (int, error) {
		return -1, unix.ENOSYS
	}
	_, err := openRecoveryWithOperations(context.Background(), Config{Root: "/stage"}, ops)
	if !errors.Is(err, errs.New(errs.KindNotImplemented, "")) {
		t.Fatalf("OpenRecovery() error = %v, want not_implemented", err)
	}
}

// Rationale: configured-root resolution must reject ordinary symlinks and
// procfs magic links before checking privileged root-owned metadata.
func TestNewRejectsSymlinkAndMagicLinkRoots(t *testing.T) {
	outside := t.TempDir()
	symlink := filepath.Join(t.TempDir(), "stage")
	if err := os.Symlink(outside, symlink); err != nil {
		t.Fatalf("create symlink fixture: %v", err)
	}
	if _, err := OpenRecovery(context.Background(), Config{Root: symlink}); err == nil {
		t.Fatal("OpenRecovery() accepted a symlink root")
	}
	magicLink := filepath.Join(t.TempDir(), "magic")
	if err := os.Symlink("/proc/self/fd/1", magicLink); err != nil {
		t.Fatalf("create magic-link fixture: %v", err)
	}
	if _, err := OpenRecovery(context.Background(), Config{Root: magicLink}); err == nil {
		t.Fatal("OpenRecovery() accepted a procfs magic-link root")
	}
}

// Rationale: procfs descriptor links are kernel magic links, so the configured
// root resolver must reject the real proc entry rather than only a test symlink.
func TestNewRejectsRealProcMagicLinkRoot(t *testing.T) {
	root := t.TempDir()
	fd := openTestDirectory(t, root)
	defer func() {
		if err := unix.Close(fd); err != nil {
			t.Errorf("close proc magic-link fixture: %v", err)
		}
	}()
	if _, err := OpenRecovery(context.Background(), Config{Root: fmt.Sprintf("/proc/self/fd/%d", fd)}); err == nil {
		t.Fatal("OpenRecovery() accepted a real procfs magic-link root")
	}
}

// Rationale: a renameat2 probe is valid only when deliberately invalid file
// descriptors produce EBADF; every other response is a failed baseline probe.
func TestRenameat2ProbeAcceptsOnlyInducedEBADF(t *testing.T) {
	for _, result := range []error{nil, unix.EPERM, unix.EINVAL} {
		ops := defaultLinuxOperations()
		ops.renameat2 = func(int, string, int, string, uint) error { return result }
		if err := probeRenameat2(context.Background(), ops); !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("probeRenameat2() error = %v for result %v, want internal", err, result)
		}
	}
}

// Rationale: reservation-lock contention must remain cancellable instead of
// blocking a worker indefinitely inside flock.
func TestReservationRootLockHonorsCancellation(t *testing.T) {
	ops := defaultLinuxOperations()
	ops.flock = func(int, int) error { return unix.EWOULDBLOCK }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := lockWithContext(ctx, 1, ops); !errors.Is(err, context.Canceled) {
		t.Fatalf("lockWithContext() error = %v, want cancellation", err)
	}
}

// Rationale: raw cleanup timeouts, unlock failures, and corrupt reservation
// bytes are private infrastructure failures with the exact Internal kind.
func TestPrivateStagingFailuresHaveExactInternalKind(t *testing.T) {
	assertInternal := func(t *testing.T, err error) {
		t.Helper()
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindInternal {
			t.Fatalf("error = %v, kind = %v/%t, want Internal", err, kind, ok)
		}
	}
	assertInternal(t, joinPrivate(context.DeadlineExceeded))
	ops := defaultLinuxOperations()
	ops.flock = func(int, int) error { return unix.EIO }
	assertInternal(t, unlockWithPrimary(context.Background(), nil, 1, ops))
	file, err := os.CreateTemp(t.TempDir(), "corrupt-reservation")
	if err != nil {
		t.Fatalf("create corrupt reservation: %v", err)
	}
	if _, err := file.Write([]byte{1}); err != nil {
		t.Fatalf("write corrupt reservation: %v", err)
	}
	assertInternal(t, func() error {
		_, err := readReservation(context.Background(), int(file.Fd()))
		return err
	}())
	if err := file.Close(); err != nil {
		t.Fatalf("close corrupt reservation: %v", err)
	}
}

// Rationale: a second worker must not obtain a concurrent lease for the same
// deterministic task, step, and point namespace.
func TestPrepareRejectsDuplicateLiveLeaseBeforeFilesystemAccess(t *testing.T) {
	ids := IDs{Task: "task", Step: "step", Point: "point"}
	stager := &Stager{
		rootFD: 0,
		leases: map[IDs]*Stage{ids: {}},
		ops:    defaultLinuxOperations(),
	}
	if _, err := stager.Prepare(context.Background(), ids, Bounded(1)); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("Prepare() error = %v, want state conflict", err)
	}
}

// Rationale: Abort must unlink a partial artifact even when its descriptor was
// already closed by an earlier failed Publish phase.
func TestAbortRemovesClosedUnpublishedPartial(t *testing.T) {
	stage, artifact, partial := newSyntheticOpenArtifact(t)
	if err := artifact.file.Close(); err != nil {
		t.Fatalf("close partial fixture: %v", err)
	}
	artifact.file = nil
	artifact.state = artifactPartial
	if err := artifact.Abort(context.Background()); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if _, err := os.Lstat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial artifact stat error = %v, want absent", err)
	}
	closeSyntheticStage(t, stage)
}

// Rationale: Cleanup owns abandoned open descriptors and plaintext names, so
// caller omission of Abort cannot retain or leak a partial artifact.
func TestCleanupClosesAndRemovesTrackedOpenArtifact(t *testing.T) {
	stage, artifact, partial := newSyntheticOpenArtifact(t)
	point := filepath.Dir(partial)
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if artifact.file != nil {
		t.Fatal("Cleanup() retained an open artifact descriptor")
	}
	if _, err := os.Lstat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial artifact stat error = %v, want absent", err)
	}
	if _, err := os.Lstat(point); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("point directory stat error = %v, want absent", err)
	}
}

func newSyntheticOpenArtifact(t *testing.T) (*Stage, *Artifact, string) {
	t.Helper()
	root := t.TempDir()
	ids := IDs{Task: "task", Step: "step", Point: "point"}
	task := filepath.Join(root, ids.Task)
	step := filepath.Join(task, ids.Step)
	point := filepath.Join(step, ids.Point)
	if err := os.MkdirAll(point, 0o700); err != nil {
		t.Fatalf("create synthetic stage: %v", err)
	}
	rootFD := openTestDirectory(t, root)
	taskFD := openTestDirectory(t, task)
	stepFD := openTestDirectory(t, step)
	pointFD := openTestDirectory(t, point)
	partial := filepath.Join(point, ".artifact.bin.partial")
	file, err := os.OpenFile(partial, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("create partial fixture: %v", err)
	}
	owner := &Stager{rootFD: -1, leases: make(map[IDs]*Stage), ops: defaultLinuxOperations()}
	stage := &Stage{
		owner: owner, rootFD: rootFD, taskFD: taskFD, stepFD: stepFD, pointFD: pointFD,
		reservationRootFD: -1, reservationTaskFD: -1, reservationStepFD: -1, reservationFD: -1,
		ids: ids, artifacts: make(map[*Artifact]struct{}),
	}
	artifact := &Artifact{
		stage: stage, file: file, temporary: ".artifact.bin.partial", final: "artifact.bin", state: artifactOpen,
	}
	stage.artifacts[artifact] = struct{}{}
	return stage, artifact, partial
}

func openTestDirectory(t *testing.T, path string) int {
	t.Helper()
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("open directory %q: %v", path, err)
	}
	return fd
}

func closeSyntheticStage(t *testing.T, stage *Stage) {
	t.Helper()
	if err := stage.Close(context.Background()); err != nil {
		t.Fatalf("Stage.Close() error = %v", err)
	}
}
