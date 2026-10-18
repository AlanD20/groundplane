package etcd

import (
	"context"

	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *AttachRepository) GetAttachFactsVersioned(
	ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
) (etcdstore.Versioned[attachrecord.EncryptedFacts], bool, error) {
	if err := attachrecord.ValidateAttachVersion(current); err != nil {
		return etcdstore.Versioned[attachrecord.EncryptedFacts]{}, false, err
	}
	revision := current.ReadRevision
	if revision == 0 {
		revision = current.Revision
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{attachrecord.AttachFactsKey(current.Record.ID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[attachrecord.EncryptedFacts]{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		if read != nil {
			etcdstore.ClearValues(read.Values)
		}
		return etcdstore.Versioned[attachrecord.EncryptedFacts]{}, false, errs.New(
			errs.KindInternal, "Attach encrypted fact read is incomplete",
		)
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil {
		if current.Record.OwnsCredential() && (len(current.Record.FactSets) != 0 || current.Record.HookBundle) {
			return etcdstore.Versioned[attachrecord.EncryptedFacts]{}, false, errs.New(
				errs.KindInternal, "Attach encrypted facts are missing",
			)
		}
		return etcdstore.Versioned[attachrecord.EncryptedFacts]{ReadRevision: revision}, false, nil
	}
	facts, err := attachrecord.DecodeAttachEncryptedFacts(read.Values[0].Value)
	if err != nil || facts.AttachID != current.Record.ID {
		clear(facts.Ciphertext)
		return etcdstore.Versioned[attachrecord.EncryptedFacts]{}, false, attachrecord.CorruptAttachRecord()
	}
	return etcdstore.Versioned[attachrecord.EncryptedFacts]{
		Record: facts, Revision: read.Values[0].ModRevision, ReadRevision: revision,
	}, true, nil
}

func (repository *AttachRepository) GetAuthoredAttachRemovalIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[environmentchanges.AttachRemovalIntent], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[environmentchanges.AttachRemovalIntent]{}, false, err
	}
	key := environmentchanges.AttachRemovalIntentKey(taskID)
	read, err := repository.store.Get(ctx, key)
	if err != nil {
		return etcdstore.Versioned[environmentchanges.AttachRemovalIntent]{}, false, err
	}
	if read == nil || read.Entry == nil {
		return etcdstore.Versioned[environmentchanges.AttachRemovalIntent]{}, false, nil
	}
	intent, err := environmentchanges.DecodeAttachRemovalIntent(read.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		environmentchanges.ClearAttachRemovalIntent(&intent)
		return etcdstore.Versioned[environmentchanges.AttachRemovalIntent]{}, false,
			errs.New(errs.KindInternal, "authored Attach removal authority is corrupt")
	}
	return etcdstore.Versioned[environmentchanges.AttachRemovalIntent]{
		Record: intent, Revision: read.Entry.ModRevision, ReadRevision: read.ReadRevision,
	}, true, nil
}
