package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupsecrets"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// restoreIdentitySessions owns request-only private bytes. Durable authority
// records contain only the selected public recipient, era and operation ids.
type restoreIdentitySessions struct {
	mu       sync.Mutex
	lifetime context.Context
	closed   bool
	sessions map[string]*restoreIdentitySession
}

type restoreIdentitySession struct {
	operationID   string
	environmentID string
	pointID       string
	keyEra        int
	recipientSHA  [sha256.Size]byte
	deadline      time.Time
	identity      []byte
	expiry        *time.Timer
}

// RetainRestoreIdentity must run before atomic Task publication. Its caller
// releases it on definite rejection, but preserves it until settlement or the
// absolute deadline when publication has an unknown outcome.
func (resolver *BackupSecretResolver) RetainRestoreIdentity(ctx context.Context,
	restore backupruntime.BackupRestoreRecord, identity []byte,
) error {
	if ctx == nil || resolver == nil || resolver.identities == nil {
		return errs.New(errs.KindInternal, "Restore identity owner is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if backupruntime.ValidateBackupRestoreRecord(restore) != nil || !restore.UsesOldIdentity ||
		restore.State != backupruntime.BackupRestoreQueued {
		return errs.New(errs.KindValidationFailed, "request-only Restore identity authority is invalid")
	}
	if len(identity) == 0 || len(identity) > int(executionplan.MaximumBackupSecretIdentityBytes) ||
		!utf8.Valid(identity) {
		return errs.New(errs.KindValidationFailed, "Restore identity must be one UTF-8 line of at most 4 KiB")
	}
	line := bytes.TrimSpace(identity)
	if len(line) == 0 || bytes.ContainsAny(line, "\r\n\x00") {
		return errs.New(errs.KindValidationFailed, "Restore identity must be one UTF-8 line of at most 4 KiB")
	}
	recipient := sha256.Sum256([]byte(restore.Point.Recipient))
	if _, err := ageinfra.IdentityRecipient(line, recipient[:]); err != nil {
		return err
	}
	deadline := restore.CreatedAt.Add(6 * time.Hour)
	sessions := resolver.identities
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if sessions.closed || sessions.lifetime.Err() != nil || !time.Now().Before(deadline) {
		return errs.New(errs.KindStateConflict, "Restore identity session is no longer available")
	}
	if _, exists := sessions.sessions[restore.TaskID]; exists {
		return errs.New(errs.KindStateConflict, "Restore identity session already exists")
	}
	session := &restoreIdentitySession{operationID: restore.OperationID, environmentID: restore.EnvironmentID,
		pointID: restore.Point.ID, keyEra: restore.Point.KeyEra, recipientSHA: recipient, deadline: deadline,
		identity: bytes.Clone(line)}
	sessions.sessions[restore.TaskID] = session
	taskID := restore.TaskID
	session.expiry = time.AfterFunc(time.Until(deadline), func() { sessions.release(taskID) })
	return nil
}

// ReleaseRestoreIdentity is idempotent and clears only this Task's volatile
// value. Channel callers invoke it after proving a durable terminal receipt.
func (resolver *BackupSecretResolver) ReleaseRestoreIdentity(ctx context.Context, taskID string) error {
	if ctx == nil || resolver == nil || resolver.identities == nil {
		return errs.New(errs.KindInternal, "Restore identity owner is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return errs.New(errs.KindValidationFailed, "Restore identity Task is invalid")
	}
	resolver.identities.release(taskID)
	return nil
}

func (sessions *restoreIdentitySessions) copy(ctx context.Context, request backupsecret.Request,
	evidence backupsecrets.Evidence,
) ([]byte, error) {
	if sessions == nil || evidence.Restore == nil || !evidence.Restore.UsesOldIdentity {
		return nil, errs.New(errs.KindStateConflict, "Restore identity lacks durable request authority")
	}
	restore := evidence.Restore
	encryption := request.Step.GetBackupStep().GetRestore().GetEncryption()
	recipient := sha256.Sum256([]byte(restore.Point.Recipient))
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	session := sessions.sessions[request.TaskID]
	if sessions.closed || sessions.lifetime.Err() != nil || session == nil || !time.Now().Before(session.deadline) {
		return nil, errs.New(
			errs.KindStateConflict,
			"old-era identity is unavailable; submit a new Restore request with its identity",
		)
	}
	if restore.TaskID != request.TaskID || session.operationID != restore.OperationID ||
		session.environmentID != restore.EnvironmentID || session.pointID != restore.Point.ID ||
		session.keyEra != restore.Point.KeyEra || !session.deadline.Equal(request.Deadline) ||
		!session.deadline.Equal(restore.CreatedAt.Add(6*time.Hour)) ||
		encryption.GetSecretSlotId() != backupsecret.OperatorOldAgeIdentitySlotID || encryption.SecretSlot != nil ||
		encryption.GetKeyEra() != uint64(session.keyEra) ||
		subtle.ConstantTimeCompare(recipient[:], session.recipientSHA[:]) != 1 ||
		subtle.ConstantTimeCompare(encryption.GetRecipientSha256(), session.recipientSHA[:]) != 1 {
		return nil, errs.New(errs.KindStateConflict, "Restore identity differs from sealed operation authority")
	}
	return bytes.Clone(session.identity), nil
}

func (sessions *restoreIdentitySessions) release(taskID string) {
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	sessions.clearLocked(taskID)
}

func (sessions *restoreIdentitySessions) clearLocked(taskID string) {
	session := sessions.sessions[taskID]
	if session == nil {
		return
	}
	session.expiry.Stop()
	clear(session.identity)
	session.identity = nil
	delete(sessions.sessions, taskID)
}

func (sessions *restoreIdentitySessions) close() {
	sessions.mu.Lock()
	defer sessions.mu.Unlock()
	sessions.closed = true
	for taskID := range sessions.sessions {
		sessions.clearLocked(taskID)
	}
}
