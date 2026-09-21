package attachments

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Reader) GetAttach(ctx context.Context, id string) (etcdstore.Versioned[Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindAttach, id); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	return recordquery.Get(
		ctx,
		repository.store,
		AttachKey(id),
		id,
		errs.KindAttachNotFound,
		DecodeAttachRecord,
		func(record Record) string { return record.ID },
	)
}

func (repository *Reader) ResolveAttach(
	ctx context.Context,
	environmentID string,
	reference string,
) (etcdstore.Versioned[Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if ids.Validate(ids.KindAttach, reference) == nil {
		current, err := repository.GetAttach(ctx, reference)
		if err != nil {
			return etcdstore.Versioned[Record]{}, err
		}
		if current.Record.EnvironmentID != environmentID {
			return etcdstore.Versioned[Record]{}, errs.New(
				errs.KindScopeUnauthorized,
				"Attach is outside the Environment scope",
			)
		}
		return current, nil
	}
	if reference == "" {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindAttachNotFound, "Attach was not found")
	}
	index, err := repository.store.Get(ctx, AttachNameKey(environmentID, reference))
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if index == nil || index.Entry == nil {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindAttachNotFound, "Attach was not found")
	}
	id := string(index.Entry.Value)
	if ids.Validate(ids.KindAttach, id) != nil {
		return etcdstore.Versioned[Record]{}, CorruptAttachRecord()
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{AttachKey(id)}, Revision: index.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return etcdstore.Versioned[Record]{}, CorruptAttachRecord()
	}
	record, err := DecodeAttachRecord(result.Values[0].Value)
	if err != nil || record.ID != id || record.EnvironmentID != environmentID || record.Name != reference {
		return etcdstore.Versioned[Record]{}, CorruptAttachRecord()
	}
	return etcdstore.Versioned[Record]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (repository *Reader) ListAttaches(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[Record], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[Record]{}, err
	}
	return recordquery.ListIndex(
		ctx,
		repository.store,
		"attaches",
		"environment",
		environmentID,
		AttachOwnerPrefix(environmentID),
		AttachKey,
		ids.KindAttach,
		request,
		DecodeAttachRecord,
		func(record Record) string { return record.ID },
		func(record Record) bool { return record.EnvironmentID == environmentID },
	)
}

func (repository *Reader) GetAttachFacts(
	ctx context.Context,
	current etcdstore.Versioned[Record],
) (EncryptedFacts, bool, error) {
	if err := ValidateAttachVersion(current); err != nil {
		return EncryptedFacts{}, false, err
	}
	revision := current.ReadRevision
	if revision == 0 {
		revision = current.Revision
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{AttachFactsKey(current.Record.ID)}, Revision: revision,
	})
	if err != nil {
		return EncryptedFacts{}, false, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		if len(current.Record.FactSets) == 0 && !current.Record.HookBundle {
			return EncryptedFacts{}, false, nil
		}
		return EncryptedFacts{}, false, errs.New(errs.KindInternal, "Attach encrypted facts are missing")
	}
	facts, err := DecodeAttachEncryptedFacts(result.Values[0].Value)
	if err != nil || facts.AttachID != current.Record.ID {
		clear(facts.Ciphertext)
		return EncryptedFacts{}, false, CorruptAttachRecord()
	}
	return facts, true, nil
}
