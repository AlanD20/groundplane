//go:build linux

package backupstage

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

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
