// Package controllerupgrade owns the closed native Controller upgrade procedure.
package controllerupgrade

import (
	"context"
	"errors"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RecoveryJournal interface {
	Current(context.Context) (upgrade.Journal, bool, error)
	Activate(context.Context, string) error
	Advance(context.Context, string, upgrade.Phase, upgrade.Phase) (upgrade.Journal, error)
	Rollback(context.Context, string) error
}

type RecoveryUnit interface {
	Stop(context.Context) error
	Start(context.Context) error
}

// Watchdog runs from the immutable predecessor under a separate transient
// service. It needs neither the candidate's API nor its configuration parser.
type Watchdog struct {
	journal RecoveryJournal
	unit    RecoveryUnit
	now     func() time.Time
}

func NewWatchdog(journal RecoveryJournal, unit RecoveryUnit) (*Watchdog, error) {
	if journal == nil || unit == nil {
		return nil, errs.New(errs.KindInternal, "controller recovery dependencies are required")
	}
	return &Watchdog{journal: journal, unit: unit, now: time.Now}, nil
}

func (watchdog *Watchdog) Run(ctx context.Context, taskID string) error {
	if ids.Validate(ids.KindTask, taskID) != nil {
		return errs.New(errs.KindValidationFailed, "controller recovery Task is invalid")
	}
	initial, err := watchdog.current(ctx, taskID)
	if err != nil {
		return err
	}
	// A replay after a long outage still gets one bounded recovery attempt;
	// systemd can retry failures without rewriting the original Task deadline.
	duration := max(
		initial.Deadline.Sub(watchdog.now()),
		upgrade.RecoveryReserveSeconds*time.Second,
	)
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	recoveryStarted := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		journal, err := watchdog.current(ctx, taskID)
		if err != nil {
			return err
		}
		if journal.Phase.Settled() {
			return nil
		}
		switch journal.Phase {
		case upgrade.PhaseActivating, upgrade.PhaseTrial:
			if !watchdog.now().Before(journal.CandidateDeadline()) {
				if err := watchdog.requestRollback(ctx, taskID); err != nil {
					return err
				}
				continue
			}
			if err := watchdog.activate(ctx, journal); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := watchdog.requestRollback(ctx, taskID); err != nil {
					return err
				}
			}
			continue
		case upgrade.PhaseStarting:
			if !watchdog.now().Before(journal.CandidateDeadline()) {
				if err := watchdog.requestRollback(ctx, taskID); err != nil {
					return err
				}
				continue
			}
		case upgrade.PhaseRollingBack:
			if err := watchdog.unit.Stop(ctx); err != nil {
				return err
			}
			if err := watchdog.journal.Rollback(ctx, taskID); err != nil {
				return err
			}
			if err := watchdog.unit.Start(ctx); err != nil {
				return err
			}
			recoveryStarted = true
			continue
		case upgrade.PhaseRolledBack:
			if !recoveryStarted {
				if err := watchdog.unit.Start(ctx); err != nil {
					return err
				}
				recoveryStarted = true
				continue
			}
		default:
			return errs.New(
				errs.KindStateConflict,
				"controller recovery has no committed activation",
			)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (watchdog *Watchdog) activate(ctx context.Context, journal upgrade.Journal) error {
	if journal.Phase == upgrade.PhaseActivating {
		if err := watchdog.unit.Stop(ctx); err != nil {
			return err
		}
		if err := watchdog.journal.Activate(ctx, journal.TaskID); err != nil {
			return err
		}
	}
	return watchdog.unit.Start(ctx)
}

func (watchdog *Watchdog) requestRollback(ctx context.Context, taskID string) error {
	journal, err := watchdog.current(ctx, taskID)
	if err != nil {
		return err
	}
	if journal.Phase.Settled() || journal.Phase == upgrade.PhaseRollingBack ||
		journal.Phase == upgrade.PhaseRolledBack {
		return nil
	}
	// Acquire rollback authority before Stop: qualification can win this CAS,
	// in which case there is no permission to interrupt the healthy process.
	_, err = watchdog.journal.Advance(ctx, taskID, journal.Phase, upgrade.PhaseRollingBack)
	if errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		return nil
	}
	return err
}

func (watchdog *Watchdog) current(ctx context.Context, taskID string) (upgrade.Journal, error) {
	journal, found, err := watchdog.journal.Current(ctx)
	if err != nil {
		return upgrade.Journal{}, err
	}
	if !found || journal.TaskID != taskID {
		return upgrade.Journal{}, errs.New(
			errs.KindStateConflict,
			"controller recovery operation changed",
		)
	}
	if err := journal.Validate(); err != nil {
		return upgrade.Journal{}, err
	}
	return journal, nil
}
