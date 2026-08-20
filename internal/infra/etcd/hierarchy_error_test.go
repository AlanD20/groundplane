package etcd

import (
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: cursor transport corruption is malformed input and must map to
// the shared validation code with HTTP 400 rather than semantic HTTP 422.
func TestMalformedCursorFailuresUseBadRequestKind(t *testing.T) {
	t.Run("malformed encoding", func(t *testing.T) {
		_, err := decodeCursor("!")
		assertErrorKindAndStatus(t, err, errs.KindMalformedRequest, 400)
	})

	t.Run("oversized encoding", func(t *testing.T) {
		_, err := decodeCursor(strings.Repeat("a", maximumEncodedCursorBytes+1))
		assertErrorKindAndStatus(t, err, errs.KindMalformedRequest, 400)
	})

	t.Run("query mismatch", func(t *testing.T) {
		cursor, err := encodeCursor(cursorPayload{
			Version: cursorVersion, Revision: 1, LastID: "not-checked", Query: "different",
		})
		if err != nil {
			t.Fatalf("encode cursor: %v", err)
		}
		_, _, _, _, err = normalizePageRequest(
			PageRequest{Limit: 50, Cursor: cursor},
			"tenants",
			"global",
			"-",
			"/v1/records/tenants/",
			ids.KindTenant,
		)
		assertErrorKindAndStatus(t, err, errs.KindMalformedRequest, 400)
	})

	t.Run("invalid last id", func(t *testing.T) {
		query, err := queryDigest("tenants", "global", "-", 50)
		if err != nil {
			t.Fatalf("query digest: %v", err)
		}
		cursor, err := encodeCursor(cursorPayload{
			Version: cursorVersion, Revision: 1, LastID: "invalid", Query: query,
		})
		if err != nil {
			t.Fatalf("encode cursor: %v", err)
		}
		_, _, _, _, err = normalizePageRequest(
			PageRequest{Limit: 50, Cursor: cursor},
			"tenants",
			"global",
			"-",
			"/v1/records/tenants/",
			ids.KindTenant,
		)
		assertErrorKindAndStatus(t, err, errs.KindMalformedRequest, 400)
	})
}

// Rationale: a decoded cursor whose page limit violates endpoint semantics is
// well-formed input and must remain a validation HTTP 422.
func TestDecodedPageLimitFailureUsesValidationKind(t *testing.T) {
	_, _, _, _, err := normalizePageRequest(
		PageRequest{Limit: MaximumPageLimit + 1},
		"tenants",
		"global",
		"-",
		"/v1/records/tenants/",
		ids.KindTenant,
	)
	assertErrorKindAndStatus(t, err, errs.KindValidationFailed, 422)
}

func assertErrorKindAndStatus(t *testing.T, err error, kind errs.Kind, status int) {
	t.Helper()
	if !errors.Is(err, errs.New(kind, "")) {
		t.Fatalf("error = %v, want Kind %d", err, kind)
	}
	var domainError *errs.Error
	if !errors.As(err, &domainError) || domainError.HTTPStatus() != status {
		t.Fatalf("error = %#v, want status %d", domainError, status)
	}
}
