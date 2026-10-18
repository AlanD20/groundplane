package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ResolveWorkSecretSlots opens plaintext only after the authenticated attempt
// and every credential lookup have survived one atomic comparison. A lost
// transaction response never authorizes delivery. Reconnect repeats the checks.
func (delivery *WorkSecretDelivery) ResolveWorkSecretSlots(ctx context.Context, authorization agentregistration.WorkAuthorization,
	attempt *agentpb.WorkAttemptIdentity, ordinal uint32,
) (*agentpb.BackupWorkInput, map[agentpb.BackupSecretSlotPurpose][]byte, error) {
	if delivery == nil || delivery.resolver == nil {
		return nil, nil, errs.New(errs.KindInternal, "Backup work credential delivery is not configured")
	}
	resolver, queue := delivery.resolver, delivery.queue
	reader := resolver.reader
	if resolver == nil || resolver.protector == nil || resolver.identities == nil || reader == nil || queue == nil {
		return nil, nil, errs.New(errs.KindInternal, "Backup work credential delivery is not configured")
	}
	resolved, err := reader.ResolveWorkSecretEvidence(ctx, queue, authorization, attempt, ordinal)
	defer resolved.Evidence.Clear()
	if err != nil {
		return nil, nil, err
	}
	grant, err := backupwork.PrepareCredentialGrant(resolved.Attempt, ordinal, resolved.Conditions)
	if err != nil {
		return nil, nil, err
	}
	defer clear(grant.Mutations[0].Value)
	committed, err := queue.Commit(ctx, grant)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return nil, nil, err
	}
	if !committed.Succeeded || !time.Now().Before(resolved.Attempt.Deadline) {
		return nil, nil, errs.New(errs.KindStateConflict, "Backup credential ownership changed before delivery")
	}
	step := resolved.Source.Input.Steps[ordinal]
	deadline := resolved.Source.Deadline
	if !time.Now().Before(deadline) {
		return nil, nil, errs.New(errs.KindStateConflict, "Backup credential source expired before delivery")
	}
	slots, err := resolver.openCapturedSlots(ctx, resolved.Attempt.TaskID, deadline, step, resolved.Evidence, resolved.Source.Purposes)
	if err != nil {
		return nil, nil, err
	}
	return resolved.Source.Input, slots, nil
}

// WorkSecretDelivery retains the same resolver and Restore identity sessions as the operator surfaces.
type WorkSecretDelivery struct {
	resolver *BackupSecretResolver
	queue    *workqueue.Repository
}

func NewWorkSecretDelivery(resolver *BackupSecretResolver, queue *workqueue.Repository) (*WorkSecretDelivery, error) {
	if resolver == nil || resolver.reader == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "Backup credential delivery dependencies are required")
	}
	return &WorkSecretDelivery{resolver: resolver, queue: queue}, nil
}
