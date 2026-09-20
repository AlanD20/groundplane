package attachments

import (
	"bytes"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
)

const RecordPrefix = "/v1/records/attaches/"

func EncodeAttachRecord(record Record) ([]byte, error) {
	if err := ValidateAttachRecord(record); err != nil {
		return nil, err
	}
	value, err := json.Marshal(record)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return value, nil
}

func DecodeAttachRecord(value []byte) (Record, error) {
	var record Record
	if err := json.Unmarshal(value, &record); err != nil {
		return Record{}, CorruptAttachRecord()
	}
	if err := ValidateAttachRecord(record); err != nil {
		return Record{}, CorruptAttachRecord()
	}
	return record, nil
}

func EncodeAttachEncryptedFacts(facts EncryptedFacts) ([]byte, error) {
	if err := ValidateAttachEncryptedFacts(facts); err != nil {
		return nil, err
	}
	value, err := json.Marshal(facts)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return value, nil
}

func DecodeAttachEncryptedFacts(value []byte) (EncryptedFacts, error) {
	var facts EncryptedFacts
	if err := json.Unmarshal(value, &facts); err != nil {
		return EncryptedFacts{}, CorruptAttachRecord()
	}
	if err := ValidateAttachEncryptedFacts(facts); err != nil {
		clear(facts.Ciphertext)
		return EncryptedFacts{}, CorruptAttachRecord()
	}
	return facts, nil
}

func CorruptAttachRecord() error {
	return errs.New(errs.KindInternal, "Attach durable state is corrupt")
}

func AttachKey(id string) string {
	return RecordPrefix + id
}

func AttachOwnerPrefix(environmentID string) string {
	return "/v1/indexes/attaches/by-owner/environment/" + environmentID + "/"
}

func AttachOwnerKey(environmentID string, attachID string) string {
	return AttachOwnerPrefix(environmentID) + attachID
}

func AttachNameKey(environmentID string, name string) string {
	return "/v1/indexes/attaches/by-name/environment/" + environmentID + "/" + recordcodec.EncodeKeySegment(name)
}

func AttachServiceKey(serviceID string, attachID string) string {
	return "/v1/indexes/attaches/by-service/service/" + serviceID + "/" + attachID
}

func AttachBackingServiceKey(serviceID string, attachID string) string {
	return "/v1/indexes/attaches/by-backing-service/service/" + serviceID + "/" + attachID
}

func AttachBackingProjectKey(projectID string, attachID string) string {
	return "/v1/indexes/attaches/by-backing-project/project/" + projectID + "/" + attachID
}

func AttachGrantedByPrefix(attachID string) string {
	return "/v1/indexes/attaches/by-granted-attach/attach/" + attachID + "/"
}

func AttachCredentialByPrefix(attachID string) string {
	return "/v1/indexes/attaches/by-credential-attach/attach/" + attachID + "/"
}

func AttachCredentialByKey(credentialAttachID string, attachID string) string {
	return AttachCredentialByPrefix(credentialAttachID) + attachID
}

func AttachFactSetsEqual(left, right []FactSetMetadata) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].GrantAttachID != right[index].GrantAttachID ||
			!slices.Equal(left[index].Facts, right[index].Facts) {
			return false
		}
	}
	return true
}

func AttachGrantedByKey(grantAttachID string, attachID string) string {
	return AttachGrantedByPrefix(grantAttachID) + attachID
}

func AttachDependentGrantPrefix(attachID string) string {
	return "/v1/indexes/attaches/by-dependent-attach/attach/" + attachID + "/"
}

func AttachDependentGrantKey(attachID string) string {
	return AttachDependentGrantPrefix(attachID) + "grants"
}

func ReadAttachDependentGrantIndex(result *etcdstore.RangeResult, attachID string) ([]string, error) {
	if result == nil || result.More || len(result.Values) > 1 {
		return nil, CorruptAttachRecord()
	}
	if len(result.Values) == 0 {
		return nil, nil
	}
	value := result.Values[0]
	if value.Key != AttachDependentGrantKey(attachID) {
		return nil, CorruptAttachRecord()
	}
	return DecodeAttachDependentGrantIndex(value.Value, attachID)
}

func ValidateAttachDependentGrantRange(
	result *etcdstore.RangeResult, attachID string, grantAttachIDs []string,
) error {
	stored, err := ReadAttachDependentGrantIndex(result, attachID)
	if err != nil || !slices.Equal(stored, grantAttachIDs) {
		return CorruptAttachRecord()
	}
	return nil
}

func EncodeAttachDependentGrantIndex(attachID string, grantAttachIDs []string) ([]byte, error) {
	if recordcodec.ValidateID(ids.KindAttach, attachID) != nil ||
		ValidateSortedStableIDs(grantAttachIDs, ids.KindAttach, "Attach dependent grant_attach_ids") != nil {
		return nil, errs.New(errs.KindValidationFailed, "Attach dependent grant index is invalid")
	}
	return json.Marshal(struct {
		AttachID       string   `json:"attach_id"`
		GrantAttachIDs []string `json:"grant_attach_ids"`
	}{AttachID: attachID, GrantAttachIDs: append([]string(nil), grantAttachIDs...)})
}

func DecodeAttachDependentGrantIndex(value []byte, attachID string) ([]string, error) {
	if recordcodec.RejectDuplicateFields(value) != nil {
		return nil, CorruptAttachRecord()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var index struct {
		AttachID       string   `json:"attach_id"`
		GrantAttachIDs []string `json:"grant_attach_ids"`
	}
	if decoder.Decode(&index) != nil || recordcodec.RequireEOF(decoder) != nil ||
		index.AttachID != attachID ||
		ValidateSortedStableIDs(index.GrantAttachIDs, ids.KindAttach, "Attach dependent grant_attach_ids") != nil {
		return nil, CorruptAttachRecord()
	}
	return index.GrantAttachIDs, nil
}

func AttachFactsKey(attachID string) string {
	return "/v1/secret-values/attach-facts/" + attachID
}
