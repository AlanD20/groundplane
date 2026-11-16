package backup

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// retainQueuedRestoreIdentity retains only the caller's private input. It does
// not select a Point, recipient, key era or runtime before prerequisites finish.
// A Controller restart intentionally loses this request-only value.
func (resolver *BackupSecretResolver) retainQueuedRestoreIdentity(ctx context.Context,
	taskID, environmentID string, deadline time.Time, identity []byte,
) error {
	if ctx == nil || resolver == nil || resolver.identities == nil || ids.Validate(ids.KindTask, taskID) != nil ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil || len(identity) == 0 ||
		len(identity) > int(executionplan.MaximumBackupSecretIdentityBytes) || !time.Now().Before(deadline) {
		return errs.New(errs.KindValidationFailed, "Queued Restore identity is invalid")
	}
	sessions := resolver.identities
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	if sessions.closed || sessions.lifetime.Err() != nil || ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Queued Restore identity session is unavailable")
	}
	if sessions.sessions[taskID] != nil {
		return errs.New(errs.KindStateConflict, "Queued Restore identity session already exists")
	}
	session := &restoreIdentitySession{
		environmentID: environmentID,
		deadline:      deadline,
		identity:      bytes.Clone(identity),
	}
	sessions.sessions[taskID] = session
	session.expiry = time.AfterFunc(time.Until(deadline), func() { sessions.release(taskID) })
	return nil
}

func (resolver *BackupSecretResolver) queuedRestoreIdentity(
	ctx context.Context,
	taskID, environmentID string,
) ([]byte, error) {
	if ctx == nil || resolver == nil || resolver.identities == nil {
		return nil, errs.New(errs.KindInternal, "Queued Restore identity owner is unavailable")
	}
	sessions := resolver.identities
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	session := sessions.sessions[taskID]
	if sessions.closed || sessions.lifetime.Err() != nil || ctx.Err() != nil || session == nil ||
		session.environmentID != environmentID || !time.Now().Before(session.deadline) {
		return nil, errs.New(
			errs.KindStateConflict,
			"The request-only Restore identity is unavailable; submit a new Restore with its private identity",
		)
	}
	return bytes.Clone(session.identity), nil
}
