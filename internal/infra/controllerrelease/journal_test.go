package controllerrelease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a separate recovery process must observe the same fsynced journal;
// preparing another Task or rewriting a frozen input cannot steal its authority.
func TestJournalPersistsAndFencesOperation(t *testing.T) {
	store, id, _ := testReleaseStore(t)
	defer store.Close()
	ctx := context.Background()
	journal := testJournal(t, store, id)
	if err := store.recordPrepared(ctx, journal); err != nil {
		t.Fatal(err)
	}
	reopened, err := openStore(ctx, store.root.Name(), store.uid)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, found, err := reopened.Current(ctx)
	if err != nil || !found || !got.SameOperation(journal) {
		t.Fatalf("reopened journal = %#v, %t, %v", got, found, err)
	}
	changed := journal
	changed.Manifest.ControllerVersion = "different"
	if err := reopened.recordPrepared(ctx, changed); err == nil {
		t.Fatal("frozen input was rewritten")
	}
	changed = journal
	changed.TaskID = ids.NewAt(ids.KindTask, journal.StartedAt, 2)
	if err := reopened.recordPrepared(ctx, changed); err == nil {
		t.Fatal("active operation was replaced")
	}
	if _, err := reopened.Advance(ctx, changed.TaskID, upgrade.PhasePrepared, upgrade.PhaseActivating); err == nil {
		t.Fatal("different Task advanced journal")
	}
}

// Rationale: cancellation racing handoff must have one winner across file
// descriptors/process owners; a committed activation is never cancelled later.
func TestJournalCancellationActivationRace(t *testing.T) {
	store, id, _ := testReleaseStore(t)
	defer store.Close()
	ctx := context.Background()
	journal := testJournal(t, store, id)
	if err := store.recordPrepared(ctx, journal); err != nil {
		t.Fatal(err)
	}
	other, err := openStore(ctx, store.root.Name(), store.uid)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for index, phase := range []upgrade.Phase{upgrade.PhaseActivating, upgrade.PhaseCancelled} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			owner := store
			if index == 1 {
				owner = other
			}
			_, err := owner.Advance(ctx, journal.TaskID, upgrade.PhasePrepared, phase)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			t.Fatal(err)
		}
	}
	if success != 1 {
		t.Fatalf("race winners = %d", success)
	}
}

func testJournal(t *testing.T, store *Store, id upgrade.Digest) upgrade.Journal {
	t.Helper()
	manifest, err := store.Inspect(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	return upgrade.Journal{
		Schema:   1,
		TaskID:   ids.NewAt(ids.KindTask, now, 1),
		Release:  id,
		Manifest: manifest,
		PreviousController: upgrade.Hash(
			[]byte("verified executable bytes"),
		),
		Phase:     upgrade.PhasePrepared,
		StartedAt: now,
		Deadline:  now.Add(600 * time.Second),
	}
}
