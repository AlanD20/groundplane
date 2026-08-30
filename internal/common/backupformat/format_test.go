package backupformat

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math"
	"testing"
)

// Rationale: age object evidence must not drift at segmentation boundaries or
// at the exact universal stored-object limit.
func TestAgeStoredSizeCanonicalVectors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		source uint64
		stored uint64
	}{
		{0, 200},
		{1, 201},
		{65_535, 65_735},
		{65_536, 65_736},
		{65_537, 65_753},
		{131_072, 131_288},
		{131_073, 131_305},
		{MaxAgeSourceBytes, MaxStoredBytes},
	}
	for _, test := range tests {
		got, err := AgeStoredSize(test.source)
		if err != nil || got != test.stored {
			t.Errorf("AgeStoredSize(%d) = %d, %v; want %d", test.source, got, err, test.stored)
		}
	}
	for _, source := range []uint64{MaxAgeSourceBytes + 1, math.MaxUint64} {
		if _, err := AgeStoredSize(source); err == nil {
			t.Errorf("AgeStoredSize(%d) accepted an invalid source", source)
		}
	}
}

// Rationale: source evidence is trusted only after exact EOF and digest
// agreement, including for a valid empty opaque source.
func TestHashAndVerifyExactEvidence(t *testing.T) {
	t.Parallel()
	content := []byte("canonical backup bytes")
	evidence, err := Hash(context.Background(), bytes.NewReader(content), MaxStoredBytes)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.SizeBytes != uint64(len(content)) || evidence.SHA256 != sha256.Sum256(content) {
		t.Fatalf("evidence = %#v", evidence)
	}
	if err := Verify(context.Background(), bytes.NewReader(content), evidence, MaxStoredBytes); err != nil {
		t.Fatal(err)
	}
	empty, err := Hash(context.Background(), bytes.NewReader(nil), 0)
	if err != nil || empty.SizeBytes != 0 || empty.SHA256 != sha256.Sum256(nil) {
		t.Fatalf("empty evidence = %#v, %v", empty, err)
	}
}

// Rationale: malformed, oversized, and digest-substituted streams must fail
// before a caller can treat their evidence as artifact authority.
func TestVerifyRejectsLengthDigestAndLimitMismatch(t *testing.T) {
	t.Parallel()
	content := []byte("abc")
	evidence := Evidence{SizeBytes: 3, SHA256: sha256.Sum256(content)}
	tests := []struct {
		name     string
		content  []byte
		evidence Evidence
		maximum  uint64
	}{
		{"truncated", []byte("ab"), evidence, 3},
		{"trailing", []byte("abcd"), evidence, 4},
		{"digest", []byte("abd"), evidence, 3},
		{"sealed limit", content, evidence, 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := Verify(
				context.Background(),
				bytes.NewReader(test.content),
				test.evidence,
				test.maximum,
			); err == nil {
				t.Fatal("Verify accepted invalid evidence")
			}
		})
	}
	if _, err := Hash(context.Background(), bytes.NewReader(content), 2); err == nil {
		t.Fatal("Hash accepted content above its limit")
	}
}
