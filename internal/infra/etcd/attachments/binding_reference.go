package attachments

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BindingReference retains resource identity and encrypted-fact identity, not
// the Task or command that originally produced them. Empty FactsSHA256 proves
// the absence of an envelope rather than accepting arbitrary later facts.
type BindingReference struct {
	AttachID      string `json:"attach_id"`
	BindingSHA256 string `json:"binding_sha256"`
	FactsSHA256   string `json:"facts_sha256,omitempty"`
}

func (reference BindingReference) Validate() error {
	if ids.Validate(ids.KindAttach, reference.AttachID) != nil || !recordcodec.ValidSHA256(reference.BindingSHA256) ||
		reference.FactsSHA256 != "" && !recordcodec.ValidSHA256(reference.FactsSHA256) {
		return errs.New(errs.KindValidationFailed, "Attach binding reference is invalid")
	}
	return nil
}

type BindingReader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

func ReadBindingReference(
	ctx context.Context,
	store BindingReader,
	attachID string,
	revision int64,
) (BindingReference, []keyvalue.Condition, error) {
	if ids.Validate(ids.KindAttach, attachID) != nil || revision <= 0 {
		return BindingReference{}, nil, errs.New(
			errs.KindValidationFailed,
			"Attach binding reference requires a fixed resource revision",
		)
	}
	keys := []string{AttachKey(attachID), AttachFactsKey(attachID)}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return BindingReference{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 || read.Values[0] == nil {
		return BindingReference{}, nil, errs.New(errs.KindStateConflict, "Attach binding reference is unavailable")
	}
	conditions := make([]keyvalue.Condition, 0, 2)
	for index, value := range read.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision) {
			return BindingReference{}, nil, errs.New(
				errs.KindStateConflict,
				"Attach binding reference revision changed",
			)
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
	}
	record, err := DecodeAttachRecord(read.Values[0].Value)
	if err != nil {
		return BindingReference{}, nil, err
	}
	digest, err := BindingDigest(record)
	if err != nil || record.ID != attachID {
		return BindingReference{}, nil, errs.New(errs.KindStateConflict, "Attach binding reference is not ready")
	}
	reference := BindingReference{AttachID: attachID, BindingSHA256: digest}
	if read.Values[1] != nil {
		facts, err := DecodeAttachEncryptedFacts(read.Values[1].Value)
		if err != nil {
			return BindingReference{}, nil, err
		}
		defer clear(facts.Ciphertext)
		if facts.AttachID != attachID {
			return BindingReference{}, nil, errs.New(errs.KindStateConflict, "Attach facts belong to another binding")
		}
		reference.FactsSHA256 = facts.CiphertextSHA256
	}
	return reference, conditions, nil
}
