package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *AttachRepository) GetAttach(ctx context.Context, id string) (etcdstore.Versioned[attachrecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindAttach, id); err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		attachrecord.AttachKey(id),
		id,
		errs.KindAttachNotFound,
		attachrecord.DecodeAttachRecord,
		func(record attachrecord.Record) string { return record.ID },
	)
}

func (repository *AttachRepository) ResolveAttach(
	ctx context.Context,
	environmentID string,
	reference string,
) (etcdstore.Versioned[attachrecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if ids.Validate(ids.KindAttach, reference) == nil {
		current, err := repository.GetAttach(ctx, reference)
		if err != nil {
			return etcdstore.Versioned[attachrecord.Record]{}, err
		}
		if current.Record.EnvironmentID != environmentID {
			return etcdstore.Versioned[attachrecord.Record]{}, errs.New(
				errs.KindScopeUnauthorized,
				"Attach is outside the Environment scope",
			)
		}
		return current, nil
	}
	if reference == "" {
		return etcdstore.Versioned[attachrecord.Record]{}, errs.New(errs.KindAttachNotFound, "Attach was not found")
	}
	index, err := repository.store.Get(ctx, attachrecord.AttachNameKey(environmentID, reference))
	if err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if index == nil || index.Entry == nil {
		return etcdstore.Versioned[attachrecord.Record]{}, errs.New(errs.KindAttachNotFound, "Attach was not found")
	}
	id := string(index.Entry.Value)
	if ids.Validate(ids.KindAttach, id) != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, attachrecord.CorruptAttachRecord()
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{attachrecord.AttachKey(id)}, Revision: index.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return etcdstore.Versioned[attachrecord.Record]{}, attachrecord.CorruptAttachRecord()
	}
	record, err := attachrecord.DecodeAttachRecord(result.Values[0].Value)
	if err != nil || record.ID != id || record.EnvironmentID != environmentID || record.Name != reference {
		return etcdstore.Versioned[attachrecord.Record]{}, attachrecord.CorruptAttachRecord()
	}
	return etcdstore.Versioned[attachrecord.Record]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (repository *AttachRepository) ListAttaches(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[attachrecord.Record], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[attachrecord.Record]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"attaches",
		"environment",
		environmentID,
		attachrecord.AttachOwnerPrefix(environmentID),
		attachrecord.AttachKey,
		ids.KindAttach,
		request,
		attachrecord.DecodeAttachRecord,
		func(record attachrecord.Record) string { return record.ID },
		func(record attachrecord.Record) bool { return record.EnvironmentID == environmentID },
	)
}

func (repository *AttachRepository) GetAttachFacts(
	ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
) (attachrecord.EncryptedFacts, bool, error) {
	if err := validateAttachVersion(current); err != nil {
		return attachrecord.EncryptedFacts{}, false, err
	}
	revision := current.ReadRevision
	if revision == 0 {
		revision = current.Revision
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{attachrecord.AttachFactsKey(current.Record.ID)}, Revision: revision,
	})
	if err != nil {
		return attachrecord.EncryptedFacts{}, false, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		if len(current.Record.FactSets) == 0 && !current.Record.HookBundle {
			return attachrecord.EncryptedFacts{}, false, nil
		}
		return attachrecord.EncryptedFacts{}, false, errs.New(errs.KindInternal, "Attach encrypted facts are missing")
	}
	facts, err := attachrecord.DecodeAttachEncryptedFacts(result.Values[0].Value)
	if err != nil || facts.AttachID != current.Record.ID {
		clear(facts.Ciphertext)
		return attachrecord.EncryptedFacts{}, false, attachrecord.CorruptAttachRecord()
	}
	return facts, true, nil
}
