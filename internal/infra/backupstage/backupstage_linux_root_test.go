//go:build linux

package backupstage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// Rationale: a prepared backup uses deterministic root-owned private metadata
// and exposes published bytes only through an anchored read descriptor.
func TestPrepareAndPublishUsesPrivateDescriptors(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	ids := IDs{
		Task:  "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Step:  "step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Point: "point_01ARZ3NDEKTSV4RRFFQ69G5FAV",
	}
	stage, err := stager.Prepare(context.Background(), ids, Bounded(1024))
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	directory := filepath.Join(root, ids.Task, ids.Step, ids.Point)
	assertPrivateMetadata(t, directory, true)

	artifact, err := stage.CreateFile(context.Background(), "artifact.bin")
	if err != nil {
		t.Fatalf("CreateFile() error = %v", err)
	}
	if _, err := artifact.Write(context.Background(), []byte("backup bytes")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	evidence, err := artifact.Publish(context.Background())
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	wantEvidence := ArtifactEvidence{
		Name: "artifact.bin", Size: uint64(len("backup bytes")), SHA256: sha256.Sum256([]byte("backup bytes")),
	}
	if evidence != wantEvidence {
		t.Fatalf("Publish() evidence = %#v, want %#v", evidence, wantEvidence)
	}
	replayed, err := artifact.Publish(context.Background())
	if err != nil || replayed != wantEvidence {
		t.Fatalf("replayed Publish() = %#v, %v; want %#v, nil", replayed, err, wantEvidence)
	}
	reader, err := artifact.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	content, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || string(content) != "backup bytes" {
		t.Fatalf("published content = %q, read error = %v, close error = %v", content, readErr, closeErr)
	}
	assertPrivateMetadata(t, filepath.Join(directory, "artifact.bin"), false)
	partial := filepath.Join(directory, ".artifact.bin.partial")
	if _, err := os.Lstat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary artifact stat error = %v, want absent", err)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("second Cleanup() error = %v, want idempotent success", err)
	}
}

// Rationale: renameat2 is part of the staging baseline and must be rejected at
// startup rather than first failing after plaintext has been written.
func TestNewReportsMissingRenameat2AtStartup(t *testing.T) {
	requireRoot(t)
	ops := defaultLinuxOperations()
	ops.renameat2 = func(int, string, int, string, uint) error {
		return unix.ENOSYS
	}
	_, err := openRecoveryWithOperations(context.Background(), Config{Root: privateRootTestFixture(t)}, ops)
	if !errors.Is(err, errs.New(errs.KindNotImplemented, "")) {
		t.Fatalf("OpenRecovery() error = %v, want not_implemented", err)
	}
}

// Rationale: an unexpected renameat2 ENOSYS after startup must leave a partial
// artifact that Abort can deterministically unlink.
func TestAbortRemovesPartialAfterRenameat2Failure(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	calls := 0
	ops := defaultLinuxOperations()
	ops.renameat2 = func(int, string, int, string, uint) error {
		calls++
		if calls == 1 {
			return unix.EBADF
		}
		return unix.ENOSYS
	}
	stager := newRootTestStagerWithOperations(t, root, ops)
	stage, artifact, directory := prepareRootArtifact(t, stager, root)
	evidence, err := artifact.Publish(context.Background())
	if err == nil {
		t.Fatal("Publish() succeeded without renameat2")
	}
	if evidence != (ArtifactEvidence{}) {
		t.Fatalf("failed Publish() evidence = %#v, want zero", evidence)
	}
	if err := artifact.Abort(context.Background()); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	partial := filepath.Join(directory, ".artifact.bin.partial")
	if _, err := os.Lstat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial artifact stat error = %v, want absent", err)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: cancellation after the atomic rename is not successful publish;
// Abort must remove the renamed but uncommitted plaintext name.
func TestAbortRemovesRenamedArtifactAfterPublishCancellation(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	publishContext, cancel := context.WithCancel(context.Background())
	ops := defaultLinuxOperations()
	ops.renameat2 = func(oldFD int, old string, newFD int, new string, flags uint) error {
		if oldFD < 0 {
			return unix.EBADF
		}
		err := unix.Renameat2(oldFD, old, newFD, new, flags)
		cancel()
		return err
	}
	stager := newRootTestStagerWithOperations(t, root, ops)
	stage, artifact, directory := prepareRootArtifact(t, stager, root)
	if _, err := artifact.Publish(publishContext); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v, want context cancellation", err)
	}
	if err := artifact.Abort(context.Background()); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	final := filepath.Join(directory, "artifact.bin")
	if _, err := os.Lstat(final); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("renamed artifact stat error = %v, want absent", err)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: an fsync failure must not strand plaintext merely because the
// underlying file descriptor was already closed while reporting the error.
func TestAbortUnlinksAfterArtifactSyncFailure(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	stage, artifact, directory := prepareRootArtifact(t, stager, root)
	if err := artifact.file.Close(); err != nil {
		t.Fatalf("close artifact fixture: %v", err)
	}
	if _, err := artifact.Publish(context.Background()); err == nil {
		t.Fatal("Publish() succeeded with a closed artifact descriptor")
	}
	_ = artifact.Abort(context.Background()) // Close may report EBADF; unlink must still be attempted.
	partial := filepath.Join(directory, ".artifact.bin.partial")
	if _, err := os.Lstat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial artifact stat error = %v, want absent", err)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: an identical namespace lease is exclusive only while live; Close
// releases it so deterministic recovery can reopen retained evidence.
func TestDuplicateLiveLeaseIsRejectedAndCloseReleasesIt(t *testing.T) {
	requireRoot(t)
	stager := newRootTestStager(t, privateRootTestFixture(t))
	ids := IDs{Task: "task", Step: "step", Point: "point"}
	first, err := stager.Prepare(context.Background(), ids, Bounded(1024))
	if err != nil {
		t.Fatalf("first Prepare() error = %v", err)
	}
	if _, err := stager.Prepare(context.Background(), ids, Bounded(1024)); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("duplicate Prepare() error = %v, want state conflict", err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	second, err := stager.Prepare(context.Background(), ids, Bounded(1024))
	if err != nil {
		t.Fatalf("Prepare() after Close error = %v", err)
	}
	if err := second.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: rejecting Stager.Close while a Stage is live must retain the
// process-wide owner flock, not merely leave Stage descriptor duplicates open.
func TestStagerCloseRetainsOwnershipUntilEveryStageIsCleaned(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	stage, err := stager.Prepare(
		context.Background(), IDs{Task: "task", Step: "step", Point: "point"}, Bounded(1024),
	)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if err := stager.Close(context.Background()); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Stager.Close() with live Stage error = %v, want state conflict", err)
	}
	blocked, err := OpenRecovery(context.Background(), Config{Root: root})
	if blocked != nil {
		if closeErr := blocked.Close(context.Background()); closeErr != nil {
			t.Errorf("unexpected blocked RecoverySession.Close() error = %v", closeErr)
		}
		t.Fatal("OpenRecovery() returned a session while the original Stager still owned the root")
	}
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("OpenRecovery() after rejected Close error = %v, want state conflict", err)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Stage.Cleanup() error = %v", err)
	}
	if err := stager.Close(context.Background()); err != nil {
		t.Fatalf("Stager.Close() after Stage cleanup error = %v", err)
	}
	restarted, err := OpenRecovery(context.Background(), Config{Root: root})
	if err != nil {
		t.Fatalf("OpenRecovery() after successful Close error = %v", err)
	}
	restartedStager, prepared, err := restarted.Complete(context.Background())
	if err != nil || len(prepared) != 0 {
		t.Fatalf("restarted Complete() = prepared %d, error %v", len(prepared), err)
	}
	if err := restartedStager.Close(context.Background()); err != nil {
		t.Fatalf("restarted Stager.Close() error = %v", err)
	}
}

// Rationale: a Stage remains in the authoritative lease map through artifact,
// reservation, growth-lock, and descriptor teardown, not merely namespace removal.
func TestStagerOwnershipCoversCleanupThroughReservationRelease(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	reachedRelease := make(chan struct{})
	continueRelease := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(continueRelease) })
	ops := defaultLinuxOperations()
	ops.beforeReservationRelease = func() {
		close(reachedRelease)
		<-continueRelease
	}
	stager := newRootTestStagerWithOperations(t, root, ops)
	stage, err := stager.Prepare(
		context.Background(), IDs{Task: "task", Step: "step", Point: "point"}, Bounded(1024),
	)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	cleanupDone := make(chan error, 1)
	go func() {
		cleanupDone <- stage.Cleanup(context.Background())
	}()
	select {
	case <-reachedRelease:
	case <-time.After(5 * time.Second):
		t.Fatal("Cleanup() did not reach the pre-reservation-release barrier")
	}
	if err := stager.Close(context.Background()); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Stager.Close() during teardown error = %v, want state conflict", err)
	}
	blocked, err := OpenRecovery(context.Background(), Config{Root: root})
	if blocked != nil {
		if closeErr := blocked.Close(context.Background()); closeErr != nil {
			t.Errorf("unexpected blocked RecoverySession.Close() error = %v", closeErr)
		}
		t.Fatal("OpenRecovery() returned a session before Stage teardown completed")
	}
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("OpenRecovery() during teardown error = %v, want state conflict", err)
	}
	releaseOnce.Do(func() { close(continueRelease) })
	if err := <-cleanupDone; err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if err := stager.Close(context.Background()); err != nil {
		t.Fatalf("Stager.Close() after teardown error = %v", err)
	}
	restarted, err := OpenRecovery(context.Background(), Config{Root: root})
	if err != nil {
		t.Fatalf("OpenRecovery() after teardown and Close error = %v", err)
	}
	restartedStager, prepared, err := restarted.Complete(context.Background())
	if err != nil || len(prepared) != 0 {
		t.Fatalf("restarted Complete() = prepared %d, error %v", len(prepared), err)
	}
	if err := restartedStager.Close(context.Background()); err != nil {
		t.Fatalf("restarted Stager.Close() error = %v", err)
	}
}

// Rationale: a post-construction Prepare failure is still an in-flight lease
// until its mandatory cleanup has released reservation and namespace authority.
func TestFailedPrepareRetainsOwnershipThroughCleanup(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	reachedRelease := make(chan struct{})
	continueRelease := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(continueRelease) })
	stager.ops.fstatfs = func(int, *unix.Statfs_t) error { return unix.EIO }
	stager.ops.beforeReservationRelease = func() {
		close(reachedRelease)
		<-continueRelease
	}
	type prepareResult struct {
		stage *Stage
		err   error
	}
	prepareDone := make(chan prepareResult, 1)
	go func() {
		stage, err := stager.Prepare(
			context.Background(), IDs{Task: "failed-task", Step: "step", Point: "point"}, Bounded(1024),
		)
		prepareDone <- prepareResult{stage: stage, err: err}
	}()
	select {
	case <-reachedRelease:
	case <-time.After(5 * time.Second):
		t.Fatal("failed Prepare cleanup did not reach the pre-reservation-release barrier")
	}
	if err := stager.Close(context.Background()); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Stager.Close() during failed Prepare cleanup error = %v, want state conflict", err)
	}
	blocked, err := OpenRecovery(context.Background(), Config{Root: root})
	if blocked != nil {
		if closeErr := blocked.Close(context.Background()); closeErr != nil {
			t.Errorf("unexpected blocked RecoverySession.Close() error = %v", closeErr)
		}
		t.Fatal("OpenRecovery() returned a session during failed Prepare cleanup")
	}
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("OpenRecovery() during failed Prepare cleanup error = %v, want state conflict", err)
	}
	releaseOnce.Do(func() { close(continueRelease) })
	result := <-prepareDone
	if result.stage != nil || result.err == nil {
		t.Fatalf("Prepare() after cleanup = %v, %v; want nil Stage and original failure", result.stage, result.err)
	}
	if !errors.Is(result.err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Prepare() error = %v, want internal fstatfs failure", result.err)
	}
	if err := stager.Close(context.Background()); err != nil {
		t.Fatalf("Stager.Close() after failed Prepare cleanup error = %v", err)
	}
	restarted, err := OpenRecovery(context.Background(), Config{Root: root})
	if err != nil {
		t.Fatalf("OpenRecovery() after failed Prepare cleanup error = %v", err)
	}
	restartedStager, prepared, err := restarted.Complete(context.Background())
	if err != nil || len(prepared) != 0 {
		t.Fatalf("restarted Complete() = prepared %d, error %v", len(prepared), err)
	}
	if err := restartedStager.Close(context.Background()); err != nil {
		t.Fatalf("restarted Stager.Close() error = %v", err)
	}
}

// Rationale: startup recovery must fail closed on every preexisting managed
// directory that does not retain its exact owner, group, and private mode.
func TestRecoveryRejectsUnexpectedDirectoryMetadata(t *testing.T) {
	requireRoot(t)
	for _, test := range []struct {
		name   string
		mutate func(string) error
	}{
		{name: "wrong mode", mutate: func(path string) error { return os.Chmod(path, 0o750) }},
		{name: "wrong owner", mutate: func(path string) error { return os.Chown(path, 1, 1) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := privateRootTestFixture(t)
			task := filepath.Join(root, "task")
			if err := os.Mkdir(task, 0o700); err != nil {
				t.Fatalf("create task: %v", err)
			}
			if err := test.mutate(task); err != nil {
				t.Fatalf("mutate task: %v", err)
			}
			session, err := OpenRecovery(context.Background(), Config{Root: root})
			if session != nil {
				if closeErr := session.Close(context.Background()); closeErr != nil {
					t.Errorf("unexpected RecoverySession.Close() error = %v", closeErr)
				}
				t.Fatal("OpenRecovery() accepted unsafe directory metadata")
			}
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("OpenRecovery() error = %v, want Internal namespace ambiguity", err)
			}
		})
	}
}

// Rationale: cleanup is not RemoveAll; hardlinked entries fail closed while
// still releasing the lease so a fresh Stage can recover after repair.
func TestCleanupReportsHardlinksAndReleasesLease(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	ids := IDs{Task: "task", Step: "step", Point: "point"}
	stage, err := stager.Prepare(context.Background(), ids, Bounded(1024))
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	directory := filepath.Join(root, ids.Task, ids.Step, ids.Point)
	unsafe := filepath.Join(directory, "unsafe")
	alias := unsafe + "-alias"
	if err := os.WriteFile(unsafe, []byte("unsafe"), 0o600); err != nil {
		t.Fatalf("write unsafe file: %v", err)
	}
	if err := os.Link(unsafe, alias); err != nil {
		t.Fatalf("create hardlink: %v", err)
	}
	if err := stage.Cleanup(context.Background()); err == nil {
		t.Fatal("Cleanup() accepted a hardlinked file")
	}
	if err := os.Remove(alias); err != nil {
		t.Fatalf("remove hardlink alias: %v", err)
	}
	recovered, err := stager.Prepare(context.Background(), ids, Bounded(1024))
	if err != nil {
		t.Fatalf("fresh Prepare() after repair error = %v", err)
	}
	if err := recovered.Cleanup(context.Background()); err != nil {
		t.Fatalf("recovered Cleanup() error = %v", err)
	}
}

// Rationale: startup recovery must reject unsafe preexisting point entries;
// it cannot defer ambiguous crash-era state until a later assignment.
func TestRecoveryRejectsUnsafePreexistingPointEntries(t *testing.T) {
	requireRoot(t)
	for _, test := range []struct {
		name   string
		create func(string) error
	}{
		{name: "symlink", create: func(path string) error { return os.Symlink("/etc/passwd", path) }},
		{name: "fifo", create: func(path string) error { return unix.Mkfifo(path, 0o600) }},
		{name: "hardlink", create: func(path string) error {
			if err := os.WriteFile(path, []byte("unsafe"), 0o600); err != nil {
				return err
			}
			return os.Link(path, path+"-alias")
		}},
		{name: "wrong mode", create: func(path string) error {
			if err := os.WriteFile(path, []byte("unsafe"), 0o600); err != nil {
				return err
			}
			return os.Chmod(path, 0o640)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := privateRootTestFixture(t)
			point := filepath.Join(root, "task", "step", "point")
			if err := os.MkdirAll(point, 0o700); err != nil {
				t.Fatalf("create point: %v", err)
			}
			if err := test.create(filepath.Join(point, "unsafe")); err != nil {
				t.Fatalf("create unsafe entry: %v", err)
			}
			session, err := OpenRecovery(context.Background(), Config{Root: root})
			if session != nil {
				if closeErr := session.Close(context.Background()); closeErr != nil {
					t.Errorf("unexpected RecoverySession.Close() error = %v", closeErr)
				}
				t.Fatal("OpenRecovery() accepted an unsafe point entry")
			}
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("OpenRecovery() error = %v, want Internal namespace ambiguity", err)
			}
		})
	}
}

// Rationale: independent Agent tasks may stage concurrently without sharing
// leases or deleting one another's deterministic namespaces.
func TestConcurrentPreparePublishAndCleanup(t *testing.T) {
	requireRoot(t)
	stager := newRootTestStager(t, privateRootTestFixture(t))
	const workers = 12
	var wait sync.WaitGroup
	errorsFound := make(chan error, workers)
	for index := 0; index < workers; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			stage, err := stager.Prepare(context.Background(), IDs{
				Task:  "task_" + string(rune('A'+index)),
				Step:  "step_" + string(rune('A'+index)),
				Point: "point_" + string(rune('A'+index)),
			}, Bounded(

				1024))

			if err != nil {
				errorsFound <- err
				return
			}
			artifact, err := stage.CreateFile(context.Background(), "artifact.bin")
			if err == nil {
				_, err = artifact.Write(context.Background(), []byte("concurrent"))
			}
			if err == nil {
				_, err = artifact.Publish(context.Background())
			}
			if err == nil {
				err = stage.Cleanup(context.Background())
			}
			if err != nil {
				errorsFound <- err
			}
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("concurrent staging error = %v", err)
	}
}

// Rationale: independent Stagers must contend on the point inode, proving the
// in-memory lease map is only an optimization.
func TestTwoStagersShareKernelLease(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	firstStager := newRootTestStager(t, root)
	secondStager := firstStager
	ids := IDs{Task: "task", Step: "step", Point: "point"}
	first, err := firstStager.Prepare(context.Background(), ids, Bounded(1024))
	if err != nil {
		t.Fatalf("first Prepare() error = %v", err)
	}
	if _, err := secondStager.Prepare(context.Background(), ids, Bounded(1024)); !errors.Is(
		err, errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("second Prepare() error = %v, want state conflict", err)
	}
	if err := first.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: same-ID goroutines deterministically produce one owner and one
// conflict instead of both entering the namespace.
func TestConcurrentSameIDPrepareHasOneWinner(t *testing.T) {
	requireRoot(t)
	stager := newRootTestStager(t, privateRootTestFixture(t))
	ids := IDs{Task: "task", Step: "step", Point: "point"}
	start := make(chan struct{})
	type result struct {
		stage *Stage
		err   error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			stage, err := stager.Prepare(context.Background(), ids, Bounded(1024))
			results <- result{stage: stage, err: err}
		}()
	}
	close(start)
	var winner *Stage
	conflicts := 0
	for range 2 {
		result := <-results
		if result.err == nil {
			winner = result.stage
		} else if errors.Is(result.err, errs.New(errs.KindStateConflict, "")) {
			conflicts++
		} else {
			t.Fatalf("Prepare() unexpected error = %v", result.err)
		}
	}
	if winner == nil || conflicts != 1 {
		t.Fatalf("winner = %p, conflicts = %d, want one each", winner, conflicts)
	}
	if err := winner.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: reserved Task spellings are rejected before root locking or path
// resolution, and therefore cannot touch an unrelated live reservation.
func TestReservedTaskNamesDoNotMutateLiveReservation(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	victim, err := stager.Prepare(
		context.Background(), IDs{Task: "victim", Step: "step", Point: "point"}, Bounded(1024),
	)
	if err != nil {
		t.Fatalf("victim Prepare() error = %v", err)
	}
	reservation := filepath.Join(root, reservationDirName, "victim", "step", "point")
	before, err := os.Stat(reservation)
	if err != nil {
		t.Fatalf("stat victim reservation: %v", err)
	}
	originalOpenat2 := stager.ops.openat2
	openCalls := 0
	stager.ops.openat2 = func(dirFD int, name string, how *unix.OpenHow) (int, error) {
		openCalls++
		return originalOpenat2(dirFD, name, how)
	}
	for _, task := range []string{
		reservationDirName,
		growthMarkerName,
		ownerMarkerName,
		".victim.partial",
		"..victim.partial",
	} {
		_, err := stager.Prepare(
			context.Background(), IDs{Task: task, Step: "step", Point: "point"}, Bounded(1),
		)
		if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("Prepare(Task: %q) error = %v, want validation", task, err)
		}
	}
	stager.ops.openat2 = originalOpenat2
	if openCalls != 0 {
		t.Fatalf("reserved Task Prepare() made %d openat2 calls, want zero", openCalls)
	}
	after, err := os.Stat(reservation)
	if err != nil {
		t.Fatalf("stat victim reservation after rejected Prepare: %v", err)
	}
	if !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		t.Fatalf("victim reservation changed: before=%v after=%v", before, after)
	}
	artifact, err := victim.CreateFile(context.Background(), "victim.bin")
	if err != nil {
		t.Fatalf("victim CreateFile() after rejected Prepare error = %v", err)
	}
	if _, err := artifact.Write(context.Background(), []byte("victim remains live")); err != nil {
		t.Fatalf("victim Write() after rejected Prepare error = %v", err)
	}
	if err := victim.Cleanup(context.Background()); err != nil {
		t.Fatalf("victim Cleanup() error = %v", err)
	}
}

// Rationale: artifact-name rejection precedes every filesystem operation, so
// recovery can never later classify an accepted final as a disposable partial.
func TestCreateFileRejectsManagedNamesBeforeFilesystemAccess(t *testing.T) {
	requireRoot(t)
	stager := newRootTestStager(t, privateRootTestFixture(t))
	stage, err := stager.Prepare(
		context.Background(), IDs{Task: "task", Step: "step", Point: "point"}, Bounded(1024),
	)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	originalOpenat2 := stager.ops.openat2
	openCalls := 0
	stager.ops.openat2 = func(dirFD int, name string, how *unix.OpenHow) (int, error) {
		openCalls++
		return originalOpenat2(dirFD, name, how)
	}
	for _, name := range []string{
		reservationDirName,
		growthMarkerName,
		ownerMarkerName,
		".artifact.bin.partial",
		"..artifact.bin.partial",
	} {
		if _, err := stage.CreateFile(context.Background(), name); !errors.Is(
			err, errs.New(errs.KindValidationFailed, ""),
		) {
			t.Fatalf("CreateFile(%q) error = %v, want validation", name, err)
		}
	}
	stager.ops.openat2 = originalOpenat2
	if openCalls != 0 {
		t.Fatalf("reserved CreateFile() made %d openat2 calls, want zero", openCalls)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

const (
	leaseHelperModeEnv  = "GROUNDPLANE_BACKUPSTAGE_LEASE_HELPER"
	leaseHelperRootEnv  = "GROUNDPLANE_BACKUPSTAGE_LEASE_ROOT"
	leaseHelperTaskEnv  = "GROUNDPLANE_BACKUPSTAGE_LEASE_TASK"
	leaseHelperSizeEnv  = "GROUNDPLANE_BACKUPSTAGE_LEASE_SIZE"
	leaseHelperReadyEnv = "GROUNDPLANE_BACKUPSTAGE_LEASE_READY"
	leaseHelperGoEnv    = "GROUNDPLANE_BACKUPSTAGE_LEASE_GO"
)

// Rationale: a fresh Agent process observes the live lease, while a later
// restart can reacquire it after descriptor release.
func TestLeaseSurvivesProcessBoundaryAndReleasesForRestart(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	stager := newRootTestStager(t, root)
	stage, err := stager.Prepare(
		context.Background(), IDs{Task: "task", Step: "step", Point: "point"}, Bounded(
			1024))

	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	runLeaseHelper(t, root, "conflict")
	if err := stage.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := stager.Close(context.Background()); err != nil {
		t.Fatalf("Stager.Close() error = %v", err)
	}
	runLeaseHelper(t, root, "acquire")
}

// Rationale: subprocess modes exercise real kernel ownership, crash release,
// and cross-process reservation evidence instead of process-local doubles.
func TestLeaseProcessHelper(t *testing.T) {
	mode := os.Getenv(leaseHelperModeEnv)
	if mode == "" {
		return
	}
	session, err := OpenRecovery(context.Background(), Config{Root: os.Getenv(leaseHelperRootEnv)})
	if mode == "conflict" || mode == "storage-conflict" {
		if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			t.Fatalf("helper OpenRecovery() error = %v, want state conflict", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("helper OpenRecovery() error = %v", err)
	}
	if mode == "pause-before-reservation" {
		session.stager.ops.beforeReserve = func() {
			if err := os.WriteFile(os.Getenv(leaseHelperReadyEnv), nil, 0o600); err != nil {
				t.Fatalf("write helper ready marker: %v", err)
			}
			for {
				if _, err := os.Stat(os.Getenv(leaseHelperGoEnv)); err == nil {
					return
				} else if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("stat helper release marker: %v", err)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}
	stager, prepared, err := session.Complete(context.Background())
	if err != nil {
		t.Fatalf("helper Complete() error = %v", err)
	}
	if len(prepared) != 0 {
		t.Fatalf("helper Complete() prepared %d stages, want none", len(prepared))
	}
	defer func() {
		if err := stager.Close(context.Background()); err != nil {
			t.Errorf("helper Stager.Close() error = %v", err)
		}
	}()
	task := os.Getenv(leaseHelperTaskEnv)
	if task == "" {
		task = "task"
	}
	required := uint64(1024)
	if encoded := os.Getenv(leaseHelperSizeEnv); encoded != "" {
		value, parseErr := strconv.ParseUint(encoded, 10, 64)
		if parseErr != nil {
			t.Fatalf("parse helper size: %v", parseErr)
		}
		required = value
	}
	capacity := Bounded(required)
	if mode == "kill-unknown-published" {
		capacity = ExclusiveUnknown()
	}
	stage, err := stager.Prepare(
		context.Background(), IDs{Task: task, Step: "step", Point: "point"}, capacity,
	)

	if err != nil {
		t.Fatalf("helper Prepare() error = %v", err)
	}
	if mode == "kill-published" || mode == "kill-unknown-published" {
		artifact, createErr := stage.CreateFile(context.Background(), "artifact.bin")
		if createErr != nil {
			t.Fatalf("helper CreateFile() error = %v", createErr)
		}
		content := make([]byte, int(required-1))
		if _, writeErr := artifact.Write(context.Background(), content); writeErr != nil {
			t.Fatalf("helper Write() error = %v", writeErr)
		}
		if _, publishErr := artifact.Publish(context.Background()); publishErr != nil {
			t.Fatalf("helper Publish() error = %v", publishErr)
		}
		if killErr := unix.Kill(os.Getpid(), unix.SIGKILL); killErr != nil {
			t.Fatalf("SIGKILL published helper: %v", killErr)
		}
		select {}
	}
	if mode == "kill-two-published" {
		for _, file := range []struct {
			name    string
			content []byte
		}{
			{name: "first.bin", content: []byte("first independently expected bytes")},
			{name: "second.bin", content: []byte("second independently expected bytes")},
		} {
			artifact, createErr := stage.CreateFile(context.Background(), file.name)
			if createErr != nil {
				t.Fatalf("helper CreateFile(%q) error = %v", file.name, createErr)
			}
			if _, writeErr := artifact.Write(context.Background(), file.content); writeErr != nil {
				t.Fatalf("helper Write(%q) error = %v", file.name, writeErr)
			}
			if _, publishErr := artifact.Publish(context.Background()); publishErr != nil {
				t.Fatalf("helper Publish(%q) error = %v", file.name, publishErr)
			}
		}
		if killErr := unix.Kill(os.Getpid(), unix.SIGKILL); killErr != nil {
			t.Fatalf("SIGKILL two-published helper: %v", killErr)
		}
		select {}
	}
	if mode == "kill-partial" {
		artifact, createErr := stage.CreateFile(context.Background(), "artifact.bin")
		if createErr != nil {
			t.Fatalf("helper CreateFile() error = %v", createErr)
		}
		if _, writeErr := artifact.Write(context.Background(), []byte("partial")); writeErr != nil {
			t.Fatalf("helper Write() error = %v", writeErr)
		}
		if killErr := unix.Kill(os.Getpid(), unix.SIGKILL); killErr != nil {
			t.Fatalf("SIGKILL partial helper: %v", killErr)
		}
		select {}
	}
	if mode == "kill" {
		if err := unix.Kill(os.Getpid(), unix.SIGKILL); err != nil {
			t.Fatalf("SIGKILL helper: %v", err)
		}
		select {}
	}
	if err := stage.Close(context.Background()); err != nil {
		t.Fatalf("helper Stage.Close() error = %v", err)
	}
}

// Rationale: sealed finals survive process death, block Ready until explicit
// Controller disposition, and resume only through retained evidence-bound fds.
func TestRecoveryInventoryResumeAndReadyGate(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	runKilledStageHelper(t, root, "kill-published", "old-task")
	session, err := OpenRecovery(context.Background(), Config{Root: root})
	if err != nil {
		t.Fatalf("OpenRecovery() error = %v", err)
	}
	inventory := session.Inventory()
	expectedBytes := make([]byte, 4095)
	expectedEvidence := ArtifactEvidence{
		Name: "artifact.bin", Size: uint64(len(expectedBytes)), SHA256: sha256.Sum256(expectedBytes),
	}
	if len(inventory) != 1 || inventory[0].IDs.Task != "old-task" || len(inventory[0].Files) != 1 ||
		inventory[0].Files[0] != expectedEvidence {
		t.Fatalf("Inventory() = %#v, want one old-task final", inventory)
	}
	if stager, prepared, err := session.Complete(
		context.Background(),
	); stager != nil || prepared != nil ||
		!errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("unresolved Complete() = %v, %v, %v; want state conflict", stager, prepared, err)
	}
	disposition := ResumeDisposition{
		RecoveryID: inventory[0].RecoveryID, DispositionID: "controller-resume-1",
		ExpectedFiles: []ArtifactEvidence{expectedEvidence}, RemainingGrowth: NoGrowth(),
	}
	if err := session.ResumePrepared(context.Background(), disposition); err != nil {
		t.Fatalf("ResumePrepared() error = %v", err)
	}
	if err := session.ResumePrepared(context.Background(), disposition); err != nil {
		t.Fatalf("idempotent ResumePrepared() error = %v", err)
	}
	stager, prepared, err := session.Complete(context.Background())
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if len(prepared) != 1 || len(prepared[0].Files) != 1 {
		t.Fatalf("prepared = %#v, want one stage/file", prepared)
	}
	reader, err := prepared[0].Files[0].Artifact.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() retained artifact error = %v", err)
	}
	content, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(content, expectedBytes) {
		t.Fatalf("retained bytes match = %t, read error = %v, close error = %v",
			bytes.Equal(content, expectedBytes), readErr, closeErr)
	}
	if _, err := prepared[0].Stage.CreateFile(
		context.Background(),
		"new.bin",
	); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("NoGrowth CreateFile() error = %v, want state conflict", err)
	}
	if err := prepared[0].Stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("resumed Cleanup() error = %v", err)
	}
	if err := stager.Close(context.Background()); err != nil {
		t.Fatalf("Stager.Close() error = %v", err)
	}
}

// Rationale: Controller recovery authority binds the exact independently known
// final count, sorted names, sizes, hashes, and bytes; any mismatch stays unresolved.
func TestRecoveryTwoFinalEvidenceMismatchesRemainUnresolved(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	runKilledStageHelper(t, root, "kill-two-published", "old-task")
	session, err := OpenRecovery(context.Background(), Config{Root: root})
	if err != nil {
		t.Fatalf("OpenRecovery() error = %v", err)
	}
	contents := map[string][]byte{
		"first.bin":  []byte("first independently expected bytes"),
		"second.bin": []byte("second independently expected bytes"),
	}
	expected := []ArtifactEvidence{
		{Name: "first.bin", Size: uint64(len(contents["first.bin"])), SHA256: sha256.Sum256(contents["first.bin"])},
		{Name: "second.bin", Size: uint64(len(contents["second.bin"])), SHA256: sha256.Sum256(contents["second.bin"])},
	}
	inventory := session.Inventory()
	if len(inventory) != 1 || len(inventory[0].Files) != len(expected) {
		t.Fatalf("Inventory() = %#v, want one recovery with two finals", inventory)
	}
	for index := range expected {
		if inventory[0].Files[index] != expected[index] {
			t.Fatalf("Inventory() file %d = %#v, want %#v", index, inventory[0].Files[index], expected[index])
		}
	}
	assertUnresolved := func(label string) {
		t.Helper()
		stager, prepared, completeErr := session.Complete(context.Background())
		if stager != nil || prepared != nil || !errors.Is(completeErr, errs.New(errs.KindStateConflict, "")) {
			t.Fatalf("Complete() after %s = %v, %v, %v; want unresolved", label, stager, prepared, completeErr)
		}
	}
	mismatches := []struct {
		name  string
		files []ArtifactEvidence
	}{
		{name: "count", files: append([]ArtifactEvidence(nil), expected[:1]...)},
		{name: "name", files: append([]ArtifactEvidence(nil), expected...)},
		{name: "order", files: []ArtifactEvidence{expected[1], expected[0]}},
	}
	mismatches[1].files[0].Name = "wrong.bin"
	for _, mismatch := range mismatches {
		err := session.ResumePrepared(context.Background(), ResumeDisposition{
			RecoveryID: inventory[0].RecoveryID, DispositionID: "bad-" + mismatch.name,
			ExpectedFiles: mismatch.files, RemainingGrowth: NoGrowth(),
		})
		if !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("ResumePrepared(%s mismatch) error = %v, want internal", mismatch.name, err)
		}
		assertUnresolved(mismatch.name + " mismatch")
	}
	if err := session.ResumePrepared(context.Background(), ResumeDisposition{
		RecoveryID: inventory[0].RecoveryID, DispositionID: "controller-resume-two",
		ExpectedFiles: expected, RemainingGrowth: NoGrowth(),
	}); err != nil {
		t.Fatalf("ResumePrepared() error = %v", err)
	}
	stager, prepared, err := session.Complete(context.Background())
	if err != nil || len(prepared) != 1 || len(prepared[0].Files) != len(expected) {
		t.Fatalf("Complete() = prepared %#v, error %v; want one stage with two finals", prepared, err)
	}
	for index, file := range prepared[0].Files {
		if file.Evidence != expected[index] {
			t.Fatalf("prepared file %d evidence = %#v, want %#v", index, file.Evidence, expected[index])
		}
		reader, openErr := file.Artifact.Open(context.Background())
		if openErr != nil {
			t.Fatalf("Open(%q) error = %v", file.Evidence.Name, openErr)
		}
		content, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(content, contents[file.Evidence.Name]) {
			t.Fatalf("read %q match = %t, read error = %v, close error = %v", file.Evidence.Name,
				bytes.Equal(content, contents[file.Evidence.Name]), readErr, closeErr)
		}
	}
	if err := prepared[0].Stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if err := stager.Close(context.Background()); err != nil {
		t.Fatalf("Stager.Close() error = %v", err)
	}
}

// Rationale: evidence mismatch cannot be adopted locally; the entry remains
// unresolved until Controller explicitly authorizes discard.
func TestRecoveryEvidenceMismatchThenDiscard(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	runKilledStageHelper(t, root, "kill-published", "old-task")
	session, err := OpenRecovery(context.Background(), Config{Root: root})
	if err != nil {
		t.Fatalf("OpenRecovery() error = %v", err)
	}
	inventory := session.Inventory()
	want := append([]ArtifactEvidence(nil), inventory[0].Files...)
	want[0].SHA256[0] ^= 0xff
	err = session.ResumePrepared(context.Background(), ResumeDisposition{
		RecoveryID: inventory[0].RecoveryID, DispositionID: "bad-evidence",
		ExpectedFiles: want, RemainingGrowth: NoGrowth(),
	})
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("ResumePrepared() mismatch error = %v, want internal", err)
	}
	if _, _, err := session.Complete(context.Background()); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Complete() after mismatch error = %v, want state conflict", err)
	}
	if err := session.DiscardRecovered(
		context.Background(), inventory[0].RecoveryID, "controller-discard-1",
	); err != nil {
		t.Fatalf("DiscardRecovered() error = %v", err)
	}
	if err := session.DiscardRecovered(
		context.Background(), inventory[0].RecoveryID, "controller-discard-1",
	); err != nil {
		t.Fatalf("idempotent DiscardRecovered() error = %v", err)
	}
	stager, prepared, err := session.Complete(context.Background())
	if err != nil || len(prepared) != 0 {
		t.Fatalf("Complete() after discard = prepared %d, error %v", len(prepared), err)
	}
	if _, err := os.Stat(filepath.Join(root, "old-task")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("discarded task stat error = %v, want absent", err)
	}
	if err := stager.Close(context.Background()); err != nil {
		t.Fatalf("Stager.Close() error = %v", err)
	}
}

// Rationale: partial bytes have never been acknowledged and are the only
// artifact state startup may remove without a Controller disposition.
func TestRecoveryCleansPartialOnlyWithoutDisposition(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	runKilledStageHelper(t, root, "kill-partial", "old-task")
	session, err := OpenRecovery(context.Background(), Config{Root: root})
	if err != nil {
		t.Fatalf("OpenRecovery() error = %v", err)
	}
	if inventory := session.Inventory(); len(inventory) != 0 {
		t.Fatalf("Inventory() = %#v, want empty after partial cleanup", inventory)
	}
	stager, prepared, err := session.Complete(context.Background())
	if err != nil || len(prepared) != 0 {
		t.Fatalf("Complete() = prepared %d, error %v", len(prepared), err)
	}
	if _, err := os.Stat(filepath.Join(root, "old-task")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial-only task stat error = %v, want absent", err)
	}
	if err := stager.Close(context.Background()); err != nil {
		t.Fatalf("Stager.Close() error = %v", err)
	}
}

// Rationale: recovery never derives remaining growth from retained file size
// or crash-era journal bytes; the explicit disposition is atomically admitted
// against current fstatfs capacity while Ready remains closed.
func TestRecoveryReadmitsExplicitBoundedCapacity(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	runKilledStageHelper(t, root, "kill-published", "old-task")
	ops := defaultLinuxOperations()
	ops.fstatfs = func(_ int, stat *unix.Statfs_t) error {
		stat.Bsize, stat.Bavail = 1, 10
		return nil
	}
	session, err := openRecoveryWithOperations(context.Background(), Config{Root: root}, ops)
	if err != nil {
		t.Fatalf("OpenRecovery() error = %v", err)
	}
	inventory := session.Inventory()
	tooLarge := ResumeDisposition{
		RecoveryID: inventory[0].RecoveryID, DispositionID: "controller-bound-11",
		ExpectedFiles: inventory[0].Files, RemainingGrowth: Bounded(11),
	}
	if err := session.ResumePrepared(
		context.Background(),
		tooLarge,
	); !errors.Is(
		err,
		errs.New(errs.KindStorageUnavailable, ""),
	) {
		t.Fatalf("ResumePrepared(Bounded(11)) error = %v, want storage unavailable", err)
	}
	admitted := tooLarge
	admitted.DispositionID, admitted.RemainingGrowth = "controller-bound-10", Bounded(10)
	if err := session.ResumePrepared(context.Background(), admitted); err != nil {
		t.Fatalf("ResumePrepared(Bounded(10)) error = %v", err)
	}
	stager, prepared, err := session.Complete(context.Background())
	if err != nil || len(prepared) != 1 {
		t.Fatalf("Complete() = prepared %d, error %v", len(prepared), err)
	}
	if prepared[0].Stage.reservationRemaining != 10 {
		t.Fatalf("recovered remaining reservation = %d, want 10", prepared[0].Stage.reservationRemaining)
	}
	if err := prepared[0].Stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if err := stager.Close(context.Background()); err != nil {
		t.Fatalf("Stager.Close() error = %v", err)
	}
}

// Rationale: publication ordering proves an unindexed partial is disposable,
// but an unindexed final is ambiguous and must be retained with startup closed.
func TestRecoveryHandlesUnindexedNamespacesFailClosed(t *testing.T) {
	requireRoot(t)
	for _, artifactName := range []string{".artifact.bin.partial", "artifact.bin"} {
		t.Run(artifactName, func(t *testing.T) {
			root := privateRootTestFixture(t)
			point := filepath.Join(root, "task", "step", "point")
			if err := os.MkdirAll(point, os.FileMode(directoryMode)); err != nil {
				t.Fatalf("create unindexed hierarchy: %v", err)
			}
			for _, directory := range []string{filepath.Join(root, "task"), filepath.Join(root, "task", "step"), point} {
				if err := os.Chmod(directory, os.FileMode(directoryMode)); err != nil {
					t.Fatalf("chmod unindexed hierarchy: %v", err)
				}
			}
			path := filepath.Join(point, artifactName)
			if err := os.WriteFile(path, []byte("bytes"), os.FileMode(fileMode)); err != nil {
				t.Fatalf("write unindexed artifact: %v", err)
			}
			if err := os.Chmod(path, os.FileMode(fileMode)); err != nil {
				t.Fatalf("chmod unindexed artifact: %v", err)
			}
			session, err := OpenRecovery(context.Background(), Config{Root: root})
			if artifactName == "artifact.bin" {
				if session != nil {
					if closeErr := session.Close(context.Background()); closeErr != nil {
						t.Errorf("unexpected RecoverySession.Close() error = %v", closeErr)
					}
				}
				if !errors.Is(err, errs.New(errs.KindInternal, "")) {
					t.Fatalf("OpenRecovery() unindexed final error = %v, want internal", err)
				}
				if _, statErr := os.Stat(path); statErr != nil {
					t.Fatalf("unindexed final was not retained: %v", statErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("OpenRecovery() partial error = %v", err)
			}
			stager, prepared, err := session.Complete(context.Background())
			if err != nil || len(prepared) != 0 {
				t.Fatalf("Complete() = prepared %d, error %v", len(prepared), err)
			}
			if _, statErr := os.Stat(filepath.Join(root, "task")); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("unindexed partial task stat error = %v, want absent", statErr)
			}
			if err := stager.Close(context.Background()); err != nil {
				t.Fatalf("Stager.Close() error = %v", err)
			}
		})
	}
}

func runKilledStageHelper(t *testing.T, root, mode, task string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	_, err = runner.New(nil).Run(context.Background(), runner.RunCmdOpts{
		Name: executable, Args: []string{"-test.run=^TestLeaseProcessHelper$"},
		Env: []string{
			leaseHelperModeEnv + "=" + mode, leaseHelperRootEnv + "=" + root,
			leaseHelperTaskEnv + "=" + task, leaseHelperSizeEnv + "=4096",
		},
	})
	if err == nil {
		t.Fatalf("%s helper exited successfully", mode)
	}
}

// Rationale: root ownership begins before point creation, so another Agent's
// mandatory recovery cannot pass Ready during the pre-reservation window.
func TestStartupBlocksDuringPrepareBeforeReservationPublication(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	signals := t.TempDir()
	ready, release := filepath.Join(signals, "helper-ready"), filepath.Join(signals, "helper-release")
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	helperErr := make(chan error, 1)
	go func() {
		_, runErr := runner.New(nil).Run(context.Background(), runner.RunCmdOpts{
			Name: executable, Args: []string{"-test.run=^TestLeaseProcessHelper$"},
			Env: []string{
				leaseHelperModeEnv + "=pause-before-reservation", leaseHelperRootEnv + "=" + root,
				leaseHelperReadyEnv + "=" + ready, leaseHelperGoEnv + "=" + release,
			},
		})
		helperErr <- runErr
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat helper ready marker: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("helper did not reach pre-reservation pause")
		}
		time.Sleep(5 * time.Millisecond)
	}
	stager, err := OpenRecovery(context.Background(), Config{Root: root})
	if stager != nil {
		if closeErr := stager.Close(context.Background()); closeErr != nil {
			t.Errorf("unexpected Stager.Close() error = %v", closeErr)
		}
	}
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("OpenRecovery() during pre-reservation pause error = %v, want state conflict", err)
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatalf("write helper release marker: %v", err)
	}
	if err := <-helperErr; err != nil {
		t.Fatalf("paused helper error = %v", err)
	}
	recovered := newRootTestStager(t, root)
	if err := recovered.Close(context.Background()); err != nil {
		t.Fatalf("recovered Stager.Close() error = %v", err)
	}
}

// Rationale: a crash after a near-bound write and publish leaves both a final
// name and stale reservation, and retry must recover the namespace before a
// fresh capacity reservation is attempted.
func TestSIGKILLAfterNearBoundPublishRecoversBeforeReservation(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	const required = uint64(1 << 20)
	_, err = runner.New(nil).Run(context.Background(), runner.RunCmdOpts{
		Name: executable, Args: []string{"-test.run=^TestLeaseProcessHelper$"},
		Env: []string{
			leaseHelperModeEnv + "=kill-published", leaseHelperRootEnv + "=" + root,
			leaseHelperSizeEnv + "=" + strconv.FormatUint(required, 10),
		},
	})
	if err == nil {
		t.Fatal("near-bound SIGKILL helper exited successfully")
	}
	oldTaskPath := filepath.Join(root, "task")
	ops := defaultLinuxOperations()
	ops.fstatfs = func(_ int, stat *unix.Statfs_t) error {
		stat.Bsize = 1
		if _, statErr := os.Stat(oldTaskPath); statErr == nil {
			stat.Bavail = 0
			return nil
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
		stat.Bavail = required
		return nil
	}
	var before unix.Statfs_t
	if err := ops.fstatfs(-1, &before); err != nil {
		t.Fatalf("pre-recovery fstatfs fixture error = %v", err)
	}
	if before.Bavail != 0 {
		t.Fatalf("pre-recovery available blocks = %d, want 0", before.Bavail)
	}
	stager := newRootTestStagerWithOperations(t, root, ops)
	available, err := availableBytes(context.Background(), stager.rootFD, ops)
	if err != nil {
		t.Fatalf("availableBytes() after recovery error = %v", err)
	}
	if available != required {
		t.Fatalf("available bytes after recovery = %d, want %d", available, required)
	}
	stage, err := stager.Prepare(
		context.Background(), IDs{Task: "fresh-task", Step: "fresh-step", Point: "fresh-point"}, Bounded(
			required))

	if err != nil {
		t.Fatalf("Prepare() after near-bound SIGKILL error = %v", err)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: SIGKILL releases the exclusive growth marker but leaves the
// reversible reservation and published inode for mandatory startup recovery;
// a fresh bounded assignment must then be admitted under different IDs.
func TestSIGKILLUnknownGrowthRecoversForFreshBoundedStage(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	_, err = runner.New(nil).Run(context.Background(), runner.RunCmdOpts{
		Name: executable, Args: []string{"-test.run=^TestLeaseProcessHelper$"},
		Env: []string{
			leaseHelperModeEnv + "=kill-unknown-published", leaseHelperRootEnv + "=" + root,
			leaseHelperTaskEnv + "=old-task", leaseHelperSizeEnv + "=4096",
		},
	})
	if err == nil {
		t.Fatal("unknown-growth SIGKILL helper exited successfully")
	}
	stager := newRootTestStager(t, root)
	if _, statErr := os.Stat(filepath.Join(root, "old-task")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("old task stat error = %v, want absent after startup recovery", statErr)
	}
	stage, err := stager.Prepare(
		context.Background(), IDs{Task: "fresh-task", Step: "fresh-step", Point: "fresh-point"}, Bounded(1),
	)
	if err != nil {
		t.Fatalf("fresh bounded Prepare() error = %v", err)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

// Rationale: kernel lease and locked reservation records must be recoverable
// after an Agent process is killed without running any defer.
func TestSIGKILLReleasesLeaseAndReapsStaleReservation(t *testing.T) {
	requireRoot(t)
	root := privateRootTestFixture(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	_, err = runner.New(nil).Run(context.Background(), runner.RunCmdOpts{
		Name: executable, Args: []string{"-test.run=^TestLeaseProcessHelper$"},
		Env: []string{leaseHelperModeEnv + "=kill", leaseHelperRootEnv + "=" + root},
	})
	if err == nil {
		t.Fatal("SIGKILL helper exited successfully")
	}
	stager := newRootTestStager(t, root)
	stage, err := stager.Prepare(
		context.Background(), IDs{Task: "task", Step: "step", Point: "point"}, Bounded(
			1024))

	if err != nil {
		t.Fatalf("Prepare() after SIGKILL error = %v", err)
	}
	if err := stage.Cleanup(context.Background()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
}

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
