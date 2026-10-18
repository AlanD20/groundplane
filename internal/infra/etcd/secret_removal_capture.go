package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secretremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *SecretRepository) CaptureSecretRemoval(ctx context.Context, owner secrets.Owner,
	current keyvalue.Versioned[secrets.Record], taskID, workID, planID string, at time.Time,
) (secretremoval.Input, error) {
	if err := secrets.ValidateSecretOwnership(ctx, owner, current.Record); err != nil {
		return secretremoval.Input{}, err
	}
	if err := secrets.ValidateSecretVersion(current); err != nil {
		return secretremoval.Input{}, err
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{secrets.ValueKey(current.Record.Secret.ID)}, Revision: current.ReadRevision})
	if err != nil {
		return secretremoval.Input{}, err
	}
	if read == nil {
		return secretremoval.Input{}, errs.New(errs.KindInternal, "Secret encrypted value capture is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != current.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return secretremoval.Input{}, errs.New(errs.KindStateConflict, "Secret encrypted value is unavailable for removal")
	}
	value, err := secrets.DecodeEncryptedValue(read.Values[0].Value)
	defer clear(value.Ciphertext)
	if err != nil || value.SecretID != current.Record.Secret.ID {
		return secretremoval.Input{}, secrets.CorruptRecord()
	}
	input := secretremoval.Input{TaskID: taskID, WorkItemID: workID, PlanID: planID, SecretID: current.Record.Secret.ID,
		SecretRevision: current.Revision, ValueRevision: read.Values[0].ModRevision, CreatedAt: at}
	if owner.Project != nil {
		input.ProjectID, input.TenantID = owner.Project.Record.ID, owner.Project.Record.TenantID
	}
	return input, input.Validate()
}
