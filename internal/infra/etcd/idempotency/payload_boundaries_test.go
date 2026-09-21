package idempotency

import (
	bytes "bytes"
	sha256 "crypto/sha256"
	hex "encoding/hex"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	testing "testing"
)

// Rationale: accepted ciphertext and replay-body ceilings are inclusive, and
// one byte over either limit must fail before transaction submission.
func TestIdempotencyMarkerPayloadBoundaries(t *testing.T) {
	t.Parallel()

	marker := testDirectMarker()
	marker.Intent.Ciphertext = bytes.Repeat([]byte{'c'}, MaximumIntentCiphertext)
	digest := sha256.Sum256(marker.Intent.Ciphertext)
	marker.Intent.CiphertextDigest = hex.EncodeToString(digest[:])
	marker.Response.Body = bytes.Repeat([]byte{'a'}, maximumReplayBody)
	marker.Response.Body[0] = '"'
	marker.Response.Body[len(marker.Response.Body)-1] = '"'
	value, err := EncodeIdempotencyMarker(marker)
	if err != nil || len(value) > maximumMarkerBytes {
		t.Fatalf("encodeIdempotencyMarker(boundary) bytes/error = %d/%v", len(value), err)
	}

	marker.Intent.Ciphertext = append(marker.Intent.Ciphertext, 'x')
	digest = sha256.Sum256(marker.Intent.Ciphertext)
	marker.Intent.CiphertextDigest = hex.EncodeToString(digest[:])
	if _, err := EncodeIdempotencyMarker(marker); !isKind(err, errs.KindInternal) {
		t.Fatalf("ciphertext over limit error = %v, want internal", err)
	}

	marker = testDirectMarker()
	marker.Response.Body = bytes.Repeat([]byte{'a'}, maximumReplayBody+1)
	marker.Response.Body[0] = '"'
	marker.Response.Body[len(marker.Response.Body)-1] = '"'
	if _, err := EncodeIdempotencyMarker(marker); !isKind(err, errs.KindInternal) {
		t.Fatalf("response over limit error = %v, want internal", err)
	}
}
