package controllerrelease

import (
	"context"
	"os"
	"testing"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
)

// Rationale: transient watchdogs do not survive host reboot. A journaled trial
// from another boot must restore the predecessor instead of running unwatched.
func TestNewBootCannotRunUnwatchedTrial(t *testing.T) {
	store, journal, previous, _ := activationStore(t)
	defer store.Close()
	ctx := context.Background()
	if err := store.Prepare(ctx, journal); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance(ctx, journal.TaskID, upgrade.PhasePrepared, upgrade.PhaseActivating); err != nil {
		t.Fatal(err)
	}
	if err := store.Activate(ctx, journal.TaskID); err != nil {
		t.Fatal(err)
	}
	store.bootID = "22222222-2222-4222-8222-222222222222"
	if err := store.Guard(ctx, journal.StartedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	assertInstalled(t, store, previous)
	assertPhase(t, store, upgrade.PhaseRolledBack)
}

// Rationale: the predecessor guard must restore service when a candidate fails
// before it can participate in recovery, without losing the owning Task record.
func TestCandidateGetsOneStartupAttempt(t *testing.T) {
	store, journal, previous, candidate := activationStore(t)
	defer store.Close()
	ctx := context.Background()
	if err := store.Prepare(ctx, journal); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance(ctx, journal.TaskID, upgrade.PhasePrepared, upgrade.PhaseActivating); err != nil {
		t.Fatal(err)
	}
	if err := store.Activate(ctx, journal.TaskID); err != nil {
		t.Fatal(err)
	}
	assertInstalled(t, store, candidate)
	if err := store.Guard(ctx, journal.StartedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	assertPhase(t, store, upgrade.PhaseStarting)
	assertInstalled(t, store, candidate)
	// A second boot models failed exec, early crash, watchdog restart or reboot.
	if err := store.Guard(ctx, journal.StartedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	assertPhase(t, store, upgrade.PhaseRolledBack)
	assertInstalled(t, store, previous)
	if err := store.Guard(ctx, journal.StartedAt.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	assertInstalled(t, store, previous)
	got, _, err := store.Current(ctx)
	if err != nil || !got.SameOperation(journal) {
		t.Fatalf("recovery changed operation: %#v, %v", got, err)
	}
}

// Rationale: power loss can occur on either side of the binary rename and on
// either side of rollback publication. Every surviving journal is recoverable.
func TestInterruptedActivationGuard(t *testing.T) {
	for _, phase := range []upgrade.Phase{upgrade.PhaseActivating, upgrade.PhaseTrial, upgrade.PhaseRollingBack} {
		t.Run(string(phase), func(t *testing.T) {
			store, journal, previous, _ := activationStore(t)
			defer store.Close()
			ctx := context.Background()
			if err := store.Prepare(ctx, journal); err != nil {
				t.Fatal(err)
			}
			journal.Phase = phase
			journal.TrialBootID = store.bootID
			if err := store.writeJournal(ctx, journal); err != nil {
				t.Fatal(err)
			}
			if err := store.Guard(ctx, journal.StartedAt.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			assertInstalled(t, store, previous)
			assertPhase(t, store, upgrade.PhaseRolledBack)
		})
	}
}

// Rationale: a fully qualified release must survive normal service restarts;
// a stale recovery helper must not replace it after successful qualification.
func TestQualifiedCandidateSurvivesGuard(t *testing.T) {
	store, journal, _, candidate := activationStore(t)
	defer store.Close()
	ctx := context.Background()
	if err := store.Prepare(ctx, journal); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance(ctx, journal.TaskID, upgrade.PhasePrepared, upgrade.PhaseActivating); err != nil {
		t.Fatal(err)
	}
	if err := store.Activate(ctx, journal.TaskID); err != nil {
		t.Fatal(err)
	}
	if err := store.Guard(ctx, journal.StartedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Advance(ctx, journal.TaskID, upgrade.PhaseStarting, upgrade.PhaseHealthy); err != nil {
		t.Fatal(err)
	}
	if err := store.Guard(ctx, journal.Deadline.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	assertInstalled(t, store, candidate)
	if err := store.Rollback(ctx, journal.TaskID); err == nil {
		t.Fatal("stale helper rolled back qualified release")
	}
}

func activationStore(t *testing.T) (*Store, upgrade.Journal, []byte, []byte) {
	t.Helper()
	store, id, candidate := testReleaseStore(t)
	path := t.TempDir()
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	store.binaries = root
	previous := []byte("previous controller executable")
	for _, name := range []string{"controller", "controller-recovery"} {
		if err := root.WriteFile(name, previous, 0o500); err != nil {
			t.Fatal(err)
		}
	}
	journal := testJournal(t, store, id)
	journal.PreviousController = upgrade.Hash(previous)
	return store, journal, previous, candidate
}

func assertInstalled(t *testing.T, store *Store, expected []byte) {
	t.Helper()
	got, err := store.binaries.ReadFile("controller")
	if err != nil || string(got) != string(expected) {
		t.Fatalf("installed = %q, %v, want %q", got, err, expected)
	}
}

func assertPhase(t *testing.T, store *Store, phase upgrade.Phase) {
	t.Helper()
	got, found, err := store.Current(context.Background())
	if err != nil || !found || got.Phase != phase {
		t.Fatalf("journal phase = %s, %t, %v; want %s", got.Phase, found, err, phase)
	}
}
