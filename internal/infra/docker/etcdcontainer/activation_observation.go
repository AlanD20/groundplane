package etcdcontainer

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/etcdactivation"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/configdocument"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (m *Manager) ObserveApply(
	ctx context.Context,
	taskID, candidate, previous string,
) (etcdactivation.Outcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.observeApply(ctx, taskID, candidate, previous)
}

// RecoverApply can restore only this operation's captured predecessor. Holding
// the lifecycle mutex joins an Apply still running in this process; bootstrap
// performs the same restoration before opening etcd after a process restart.
func (m *Manager) RecoverApply(
	ctx context.Context,
	taskID, candidate, previous string,
) (etcdactivation.Outcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, err := m.activationRecord(ctx)
	if err != nil {
		return etcdactivation.Unresolved, err
	}
	if record.TaskID == taskID && record.Phase == "prepared" {
		if record.Candidate != candidate || record.Previous != previous {
			return etcdactivation.Unresolved, errs.New(
				errs.KindStateConflict,
				"etcd activation recovery differs from captured configuration",
			)
		}
		if err := m.recoverInterrupted(ctx); err != nil {
			return etcdactivation.Unresolved, err
		}
	}
	return m.observeApply(ctx, taskID, candidate, previous)
}

func (m *Manager) observeApply(
	ctx context.Context,
	taskID, candidate, previous string,
) (etcdactivation.Outcome, error) {
	if ids.Validate(ids.KindTask, taskID) != nil {
		return etcdactivation.Unresolved, errs.New(
			errs.KindValidationFailed,
			"etcd activation Task identity is invalid",
		)
	}
	for _, content := range []string{candidate, previous} {
		if err := config.ValidateEtcdDocument(ctx, []byte(content)); err != nil {
			return etcdactivation.Unresolved, errs.Wrap(errs.KindValidationFailed, err)
		}
	}
	record, err := m.activationRecord(ctx)
	if err != nil {
		return etcdactivation.Unresolved, err
	}
	if record.TaskID != taskID {
		for _, selection := range []struct {
			store *configdocument.Store
			id    string
		}{
			{m.operation, taskID + ":prepared"}, {m.activation, taskID + ":apply"}, {m.activation, taskID + ":rollback"},
		} {
			found, err := selection.store.Replaced(ctx, selection.id)
			if err != nil {
				return etcdactivation.Unresolved, err
			}
			if found {
				return etcdactivation.Unresolved, errs.New(
					errs.KindStateConflict,
					"etcd activation receipt was replaced before execution settled",
				)
			}
		}
		return etcdactivation.NotStarted, nil
	}
	if record.Candidate != candidate || record.Previous != previous {
		return etcdactivation.Unresolved, errs.New(
			errs.KindStateConflict,
			"etcd activation receipt differs from captured configuration",
		)
	}
	var expected string
	var outcome etcdactivation.Outcome
	switch record.Phase {
	case "applied":
		expected, outcome = candidate, etcdactivation.Applied
	case "rolled_back":
		expected, outcome = previous, etcdactivation.Restored
	default:
		return etcdactivation.Unresolved, nil
	}
	active, _, _, err := m.activation.Current(ctx)
	if err != nil {
		return etcdactivation.Unresolved, err
	}
	cfg, err := config.ParseEtcdDocument(ctx, []byte(expected))
	if err != nil {
		return etcdactivation.Unresolved, errs.Wrap(errs.KindValidationFailed, err)
	}
	observed, exists, err := m.inspect(ctx)
	if err != nil {
		return etcdactivation.Unresolved, err
	}
	if active != expected || !exists || !owned(observed) || !matchesDesired(observed, cfg) || observed.State == nil ||
		!observed.State.Running {
		return etcdactivation.Unresolved, errs.New(
			errs.KindStateConflict,
			"etcd runtime does not match its activation receipt",
		)
	}
	if err := m.probe(ctx); err != nil {
		return etcdactivation.Unresolved, err
	}
	return outcome, nil
}
