//go:build linux

package backupstage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
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
