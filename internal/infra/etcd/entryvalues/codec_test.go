package entryvalues

import (
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testing "testing"
)

// Rationale: a corrupted value generation must fail before any resolver can
// expose its bytes to the transient materialization channel.
func TestEntryValueGenerationRejectsCorruptDurableDigest(t *testing.T) {
	record := testPlainEntryValueGeneration()
	encoded, err := EncodePlain(record)
	if err != nil {
		t.Fatalf("encodePlainEntryValueGeneration() error = %v", err)
	}
	data, err := testrecordcodec.Decode[plainEntryValueGenerationData](encoded, "entry_plain_value_generation")
	if err != nil {
		t.Fatalf("decodeEnvelope() error = %v", err)
	}
	data.Content[0] ^= 1
	corrupt, err := testrecordcodec.Encode("entry_plain_value_generation", data)
	if err != nil {
		t.Fatalf("encodeEnvelope() error = %v", err)
	}
	if _, err := DecodePlain(corrupt); err == nil {
		t.Fatal("decodePlainEntryValueGeneration(corrupt) error = nil")
	}
}
