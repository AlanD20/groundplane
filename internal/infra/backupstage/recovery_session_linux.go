package backupstage

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
	"strings"
)

// Inventory returns a detached, deterministic copy for Controller authority.
func (session *RecoverySession) Inventory() []Recovered {
	if session == nil {
		return nil
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	result := make([]Recovered, 0, len(session.order))
	for _, recoveryID := range session.order {
		recovered := session.recovered[recoveryID]
		entry := recovered.entry
		entry.Files = append([]ArtifactEvidence(nil), entry.Files...)
		result = append(result, entry)
	}
	return result
}

// ResumePrepared applies one explicit Controller disposition while Ready is closed.
func (session *RecoverySession) ResumePrepared(ctx context.Context, disposition ResumeDisposition) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if session == nil {
		return internalError("recovery session is required")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.completed {
		return internalError("recovery session is closed")
	}
	if strings.TrimSpace(disposition.DispositionID) == "" {
		return validationError("recovery disposition id is required")
	}
	recovered, exists := session.recovered[disposition.RecoveryID]
	if !exists {
		return validationError("recovery id is not in startup inventory")
	}
	if recovered.resolved {
		if recovered.resumed && recovered.dispositionID == disposition.DispositionID &&
			recoveryDispositionEqual(recovered.resumeDisposition, disposition) {
			return nil
		}
		return stateConflictError("recovery entry already has a disposition")
	}
	return session.resumePreparedLocked(ctx, recovered, disposition)
}

// DiscardRecovered applies explicit Controller authority and durably removes
// the namespace before its reservation marker.
func (session *RecoverySession) DiscardRecovered(
	ctx context.Context,
	recoveryID RecoveryID,
	dispositionID string,
) error {
	if session == nil {
		return joinPrivate(contextError(ctx), internalError("recovery session is required"))
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.completed {
		return joinPrivate(contextError(ctx), internalError("recovery session is closed"))
	}
	if strings.TrimSpace(dispositionID) == "" {
		return joinPrivate(contextError(ctx), validationError("recovery disposition id is required"))
	}
	recovered, exists := session.recovered[recoveryID]
	if !exists {
		return joinPrivate(contextError(ctx), validationError("recovery id is not in startup inventory"))
	}
	if recovered.resolved {
		if !recovered.resumed && recovered.dispositionID == dispositionID {
			return contextError(ctx)
		}
		return joinPrivate(contextError(ctx), stateConflictError("recovery entry already has a disposition"))
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), session.stager.ops.cleanupTimeout)
	defer cancel()
	if err := session.discardRecoveredLocked(cleanupCtx, recovered); err != nil {
		return joinPrivate(contextError(ctx), err)
	}
	recovered.resolved = true
	recovered.dispositionID = dispositionID
	return contextError(ctx)
}

// Complete is the only transition that exposes assignment Prepare.
func (session *RecoverySession) Complete(ctx context.Context) (*Stager, []*PreparedStage, error) {
	if err := contextError(ctx); err != nil {
		return nil, nil, err
	}
	if session == nil {
		return nil, nil, internalError("recovery session is required")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.completed {
		return nil, nil, internalError("recovery session is closed")
	}
	for _, recoveryID := range session.order {
		if !session.recovered[recoveryID].resolved {
			return nil, nil, stateConflictError("startup recovery has unresolved Controller dispositions")
		}
	}
	closeErr := closeFDs(ctx, session.reservationRootFD)
	session.reservationRootFD = -1
	unlockErr := rawOperationError("unlock startup recovery root",
		session.stager.ops.flock(session.stager.rootFD, unix.LOCK_UN))
	if unlockErr != nil {
		return nil, nil, errs.WrapJoined(errs.KindInternal, closeErr, unlockErr)
	}
	if closeErr != nil {
		return nil, nil, closeErr
	}
	session.rootLocked, session.completed = false, true
	prepared := append([]*PreparedStage(nil), session.prepared...)
	return session.stager, prepared, nil
}

// Close abandons the boot attempt without deleting retained finals.
func (session *RecoverySession) Close(ctx context.Context) error {
	if session == nil {
		return contextError(ctx)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.completed {
		return contextError(ctx)
	}
	session.closed = true
	return closeRecoveryLocked(ctx, session)
}
