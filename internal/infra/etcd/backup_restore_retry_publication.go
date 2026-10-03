package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (publication *PreparedConfigRestorePublication) BindRetry(source PreparedRestoreRetrySource) error {
	if publication == nil || publication.state == nil {
		return errs.New(errs.KindInternal, "config Restore retry publication is not prepared")
	}
	state := publication.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.repository == nil || state.retryOf != "" {
		return errs.New(errs.KindStateConflict, "config Restore retry publication was consumed")
	}
	conditions, attempt, err := backupruntime.BindRestoreRetry(
		source.RestoreRetrySnapshot,
		state.restore,
		state.scope,
		state.authority,
		nil,
		state.conditions,
		state.mutations,
	)
	if err != nil {
		return err
	}
	state.conditions, state.retryOf = conditions, source.Task.ID
	state.scope.TaskAttempt = attempt
	return nil
}

func (publication *PreparedVolumeRestorePublication) BindRetry(source PreparedRestoreRetrySource) error {
	if publication == nil || publication.state == nil {
		return errs.New(errs.KindInternal, "Volume Restore retry publication is not prepared")
	}
	state := publication.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.repository == nil || state.retryOf != "" {
		return errs.New(errs.KindStateConflict, "Volume Restore retry publication was consumed")
	}
	conditions, attempt, err := backupruntime.BindRestoreRetry(
		source.RestoreRetrySnapshot,
		state.restore,
		state.scope,
		state.authority,
		state.artifacts,
		state.conditions,
		state.mutations,
	)
	if err != nil {
		return err
	}
	state.conditions, state.retryOf = conditions, source.Task.ID
	state.scope.TaskAttempt = attempt
	return nil
}

func (publication *PreparedPostgresRestorePublication) BindRetry(source PreparedRestoreRetrySource) error {
	if publication == nil || publication.state == nil {
		return errs.New(errs.KindInternal, "PostgreSQL Restore retry publication is not prepared")
	}
	state := publication.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.repository == nil || state.retryOf != "" {
		return errs.New(errs.KindStateConflict, "PostgreSQL Restore retry publication was consumed")
	}
	conditions, attempt, err := backupruntime.BindRestoreRetry(
		source.RestoreRetrySnapshot,
		state.restore,
		state.scope,
		state.authority,
		state.artifacts,
		state.conditions,
		state.mutations,
	)
	if err != nil {
		return err
	}
	state.conditions, state.retryOf = conditions, source.Task.ID
	state.scope.TaskAttempt = attempt
	return nil
}
