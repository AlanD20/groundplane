package attachments

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// AccessFactSource holds the consumer, direct credential owner, selected grant
// and encrypted facts from one MVCC view. It contains no resolved plaintext.
// Its conditions must be included in the check's atomic admission publication.
type AccessFactSource struct {
	Consumer   keyvalue.Versioned[Record]
	Owner      keyvalue.Versioned[Record]
	Grant      *keyvalue.Versioned[Record]
	Facts      keyvalue.Versioned[EncryptedFacts]
	conditions []keyvalue.Condition
}

func (source AccessFactSource) Conditions() []keyvalue.Condition {
	return slices.Clone(source.conditions)
}

func (source *AccessFactSource) Clear() {
	clear(source.Facts.Record.Ciphertext)
	source.Facts.Record.Ciphertext = nil
}

func (repository *Reader) CaptureAccessFacts(
	ctx context.Context, consumer keyvalue.Versioned[Record], grantID string,
) (AccessFactSource, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return AccessFactSource{}, err
	}
	if ValidateAttachVersion(consumer) != nil || consumer.ReadRevision <= 0 ||
		grantID != "" && ids.Validate(ids.KindAttach, grantID) != nil {
		return AccessFactSource{}, errs.New(errs.KindValidationFailed, "Backing access fact selection is invalid")
	}
	ownerID := consumer.Record.CredentialAttachID
	keys := []string{AttachKey(consumer.Record.ID), AttachKey(ownerID), AttachFactsKey(ownerID)}
	if grantID != "" {
		keys = append(keys, AttachKey(grantID))
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: consumer.ReadRevision})
	if err != nil {
		return AccessFactSource{}, err
	}
	if read == nil || read.ReadRevision != consumer.ReadRevision || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return AccessFactSource{}, errs.New(errs.KindInternal, "Backing access fact snapshot is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for _, value := range read.Values {
		if value == nil {
			return AccessFactSource{}, errs.New(
				errs.KindStateConflict,
				"Backing access credential or grant is unavailable",
			)
		}
	}
	selected, err := DecodeAttachRecord(read.Values[0].Value)
	if err != nil {
		return AccessFactSource{}, err
	}
	if read.Values[0].ModRevision != consumer.Revision || selected.ID != consumer.Record.ID ||
		selected.EnvironmentID != consumer.Record.EnvironmentID || selected.ServiceID != consumer.Record.ServiceID ||
		selected.CredentialAttachID != ownerID {
		return AccessFactSource{}, errs.New(
			errs.KindStateConflict,
			"consumer Attach changed before its access check was captured",
		)
	}
	owner, err := DecodeAttachRecord(read.Values[1].Value)
	if err != nil {
		return AccessFactSource{}, err
	}
	if owner.ID != ownerID || !owner.OwnsCredential() || owner.EnvironmentID != selected.EnvironmentID ||
		owner.BackingServiceID != selected.BackingServiceID || owner.BackingNetworkID != selected.BackingNetworkID {
		return AccessFactSource{}, errs.New(
			errs.KindStateConflict,
			"Backing access credential owner does not match the consumer",
		)
	}
	source := AccessFactSource{
		Consumer: keyvalue.Versioned[Record]{
			Record:       selected,
			Revision:     read.Values[0].ModRevision,
			ReadRevision: read.ReadRevision,
		},
		Owner: keyvalue.Versioned[Record]{
			Record:       owner,
			Revision:     read.Values[1].ModRevision,
			ReadRevision: read.ReadRevision,
		},
	}
	if grantID != "" {
		grant, err := DecodeAttachRecord(read.Values[3].Value)
		if err != nil {
			return AccessFactSource{}, err
		}
		if grant.ID != grantID || grant.EnvironmentID != selected.EnvironmentID ||
			grant.BackingServiceID != owner.BackingServiceID || !slices.Contains(owner.GrantAttachIDs, grantID) {
			return AccessFactSource{}, errs.New(
				errs.KindScopeUnauthorized,
				"Backing access grant is not part of the captured credential",
			)
		}
		source.Grant = &keyvalue.Versioned[Record]{
			Record:       grant,
			Revision:     read.Values[3].ModRevision,
			ReadRevision: read.ReadRevision,
		}
	}
	facts, err := DecodeAttachEncryptedFacts(read.Values[2].Value)
	if err != nil {
		return AccessFactSource{}, err
	}
	if facts.AttachID != ownerID {
		clear(facts.Ciphertext)
		return AccessFactSource{}, CorruptAttachRecord()
	}
	source.Facts = keyvalue.Versioned[EncryptedFacts]{
		Record:       facts,
		Revision:     read.Values[2].ModRevision,
		ReadRevision: read.ReadRevision,
	}
	// A credential-owning consumer selects the same Attach twice. Keep one
	// comparison per stored record without dropping any distinct grant evidence.
	compared := make(map[string]int64, len(keys))
	for index, key := range keys {
		revision := read.Values[index].ModRevision
		if previous, found := compared[key]; found {
			if previous != revision {
				source.Clear()
				return AccessFactSource{}, errs.New(
					errs.KindInternal,
					"Backing access snapshot has conflicting record revisions",
				)
			}
			continue
		}
		compared[key] = revision
		source.conditions = append(source.conditions, keyvalue.Condition{Key: key, ModRevision: revision})
	}
	return source, nil
}
