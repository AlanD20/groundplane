package tasksecretpinrecord

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: SEC-07 permits only an exact operation/Secret/revision/digest
// membership record; this metadata-only shape retains the source identity
// needed by SVC-15 and JOURNEY-02 without persisting value bytes.
func TestRecordCodecIsValidatedAndMetadataOnly(t *testing.T) {
	at := time.Date(2026, 9, 14, 15, 0, 0, 0, time.UTC)
	digest := sha256.Sum256([]byte("opaque ciphertext"))
	record := Record{
		OperationID: ids.NewAt(ids.KindOperation, at, 1),
		SecretID:    ids.NewAt(ids.KindSecret, at, 2), MetadataRevision: 19,
		CiphertextSHA256: hex.EncodeToString(digest[:]),
	}
	encoded, err := Encode(record)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"operation_id":"` + record.OperationID + `","secret_id":"` + record.SecretID +
		`","metadata_revision":19,"ciphertext_sha256":"` + record.CiphertextSHA256 + `"}`
	if string(encoded) != want {
		t.Fatalf("encoded recovery pin = %s", encoded)
	}
	decoded, err := Decode(encoded)
	if err != nil || decoded != record {
		t.Fatalf("Decode() = %#v/%v", decoded, err)
	}

	invalid := record
	invalid.MetadataRevision = 0
	if _, err := Encode(invalid); !errorsIsKind(err, errs.KindValidationFailed) {
		t.Fatalf("zero metadata revision = %v", err)
	}
	unknown := append(encoded[:len(encoded)-1], []byte(`,"ciphertext":"forbidden"}`)...)
	if _, err := Decode(unknown); !errorsIsKind(err, errs.KindInternal) {
		t.Fatalf("unexpected value field = %v", err)
	}
	duplicate := []byte(`{"operation_id":"` + record.OperationID + `","operation_id":"` +
		ids.NewAt(ids.KindOperation, at, 3) + `","secret_id":"` + record.SecretID +
		`","metadata_revision":19,"ciphertext_sha256":"` + record.CiphertextSHA256 + `"}`)
	if _, err := Decode(duplicate); !errorsIsKind(err, errs.KindInternal) {
		t.Fatalf("duplicate operation identity = %v", err)
	}
}

func errorsIsKind(err error, kind errs.Kind) bool {
	actual, found := errs.KindOf(err)
	return found && actual == kind
}
