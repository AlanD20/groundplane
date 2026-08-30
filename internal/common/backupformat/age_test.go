package backupformat

import "testing"

// Rationale: the universal age geometry is storage authority shared by every
// artifact kind, including the zero-length source case and chunk boundary.
func TestAgeStoredSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		source uint64
		stored uint64
	}{
		{source: 0, stored: 200},
		{source: 1, stored: 201},
		{source: 65_535, stored: 65_735},
		{source: 65_536, stored: 65_736},
		{source: 65_537, stored: 65_753},
		{source: MaxAgeSourceBytes, stored: MaxStoredBytes},
	}
	for _, test := range tests {
		got, err := AgeStoredSize(test.source)
		if err != nil {
			t.Fatalf("AgeStoredSize(%d) error = %v", test.source, err)
		}
		if got != test.stored {
			t.Fatalf("AgeStoredSize(%d) = %d; want %d", test.source, got, test.stored)
		}
	}
	if _, err := AgeStoredSize(MaxAgeSourceBytes + 1); err == nil {
		t.Fatal("AgeStoredSize accepted a source above the universal ceiling")
	}
}
