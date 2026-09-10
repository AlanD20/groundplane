package controllerupgrade

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: recovery is performed by the predecessor process, including when
// the candidate cannot start or cannot prove readiness before its deadline.
func TestWatchdogActivationAndRecovery(t *testing.T) {
	for _, mode := range []string{"healthy", "bad-start", "bad-copy", "deadline", "rollback-resume"} {
		t.Run(mode, func(t *testing.T) {
			journal := watchdogJournal()
			store := &watchdogStore{journal: journal, mode: mode}
			unit := &watchdogUnit{store: store}
			dog, err := NewWatchdog(store, unit)
			if err != nil {
				t.Fatal(err)
			}
			dog.now = func() time.Time { return journal.StartedAt.Add(time.Second) }
			if mode == "deadline" {
				store.journal.Phase = upgrade.PhaseStarting
				dog.now = func() time.Time { return journal.Deadline }
			}
			if mode == "rollback-resume" {
				store.journal.Phase = upgrade.PhaseRollingBack
			}
			if err := dog.Run(context.Background(), journal.TaskID); err != nil {
				t.Fatal(err)
			}
			want := upgrade.PhaseRecovered
			if mode == "healthy" {
				want = upgrade.PhaseHealthy
			}
			if store.journal.Phase != want {
				t.Fatalf("phase=%s, want %s", store.journal.Phase, want)
			}
			if mode != "healthy" && store.rollbacks != 1 {
				t.Fatalf("rollback count=%d", store.rollbacks)
			}
			if unit.starts == 0 || unit.stops == 0 {
				t.Fatalf("unit calls: start=%d stop=%d", unit.starts, unit.stops)
			}
		})
	}
}

// Rationale: qualification may win the exact timeout race. Claim rollback
// before stopping the unit, so a stale timer cannot shut down a healthy winner.
func TestWatchdogDoesNotStopQualificationWinner(t *testing.T) {
	journal := watchdogJournal()
	journal.Phase = upgrade.PhaseStarting
	store := &watchdogStore{journal: journal, mode: "qualification-race"}
	unit := &watchdogUnit{store: store}
	dog, err := NewWatchdog(store, unit)
	if err != nil {
		t.Fatal(err)
	}
	dog.now = func() time.Time { return journal.Deadline }
	if err := dog.Run(context.Background(), journal.TaskID); err != nil {
		t.Fatal(err)
	}
	if unit.stops != 0 || unit.starts != 0 || store.rollbacks != 0 {
		t.Fatal("stale watchdog interrupted qualified Controller")
	}
}

type watchdogStore struct {
	journal   upgrade.Journal
	mode      string
	rollbacks int
}

func (store *watchdogStore) Current(context.Context) (upgrade.Journal, bool, error) {
	return store.journal, true, nil
}
func (store *watchdogStore) Activate(context.Context, string) error {
	if store.mode == "bad-copy" {
		return errors.New("copy failed")
	}
	store.journal.Phase = upgrade.PhaseTrial
	return nil
}

func (store *watchdogStore) Advance(
	_ context.Context,
	id string,
	from, to upgrade.Phase,
) (upgrade.Journal, error) {
	if store.mode == "qualification-race" {
		store.journal.Phase = upgrade.PhaseHealthy
	}
	if store.journal.TaskID != id || store.journal.Phase != from {
		return upgrade.Journal{}, errs.New(errs.KindStateConflict, "changed")
	}
	store.journal.Phase = to
	return store.journal, nil
}
func (store *watchdogStore) Rollback(context.Context, string) error {
	store.rollbacks++
	store.journal.Phase = upgrade.PhaseRolledBack
	return nil
}

type watchdogUnit struct {
	store         *watchdogStore
	starts, stops int
}

func (unit *watchdogUnit) Stop(context.Context) error { unit.stops++; return nil }
func (unit *watchdogUnit) Start(context.Context) error {
	unit.starts++
	if unit.store.journal.Phase == upgrade.PhaseRolledBack {
		unit.store.journal.Phase = upgrade.PhaseRecovered
		return nil
	}
	if unit.store.mode == "bad-start" {
		return errors.New("candidate cannot execute")
	}
	unit.store.journal.Phase = upgrade.PhaseHealthy
	return nil
}
func watchdogJournal() upgrade.Journal {
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	return upgrade.Journal{
		Schema:             1,
		TaskID:             ids.NewAt(ids.KindTask, now, 1),
		Release:            upgrade.Hash([]byte("release")),
		PreviousController: upgrade.Hash([]byte("previous")),
		Phase:              upgrade.PhaseActivating,
		Manifest: upgrade.Manifest{
			Schema:            1,
			ControllerSHA256:  upgrade.Hash([]byte("candidate")),
			ControllerVersion: "0.1.0",
			AgentImage: "registry.example/agent@sha256:" + strings.Repeat(
				"a",
				64,
			),
			StorageEpoch:  1,
			ChannelSchema: 1,
		},
		StartedAt:   now,
		Deadline:    now.Add(600 * time.Second),
		TrialBootID: "11111111-1111-4111-8111-111111111111",
	}
}
