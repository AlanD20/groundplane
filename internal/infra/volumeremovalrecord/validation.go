package volumeremovalrecord

import (
	"encoding/base64"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReplayLocator is the immutable original response identity. It carries no
// store or Task repository dependency; adapters bind it to their marker API.
type ReplayLocator struct {
	ScopeKind string
	ScopeID   string
	Method    string
	Route     string
	Key       string
}

var replayKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{16,128}$`)

func validateReplayLocator(locator ReplayLocator) error {
	if locator.ScopeKind != "environment" || ids.Validate(ids.KindEnvironment, locator.ScopeID) != nil ||
		!strings.HasPrefix(
			locator.Route,
			"/",
		) || !utf8.ValidString(locator.Route) || !replayKeyPattern.MatchString(locator.Key) {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal replay locator is invalid")
	}
	switch locator.Method {
	case "POST", "PUT", "PATCH", "DELETE":
	default:
		return errs.New(errs.KindValidationFailed, "Environment Volume removal replay method is invalid")
	}
	// Preserve the existing marker-key ceiling without granting this record
	// package marker persistence or lookup authority.
	keyBytes := len("/v1/runtime/idempotency/environment/") + len(locator.ScopeID) + 3 +
		len(encodeSegment(locator.Method)) + len(encodeSegment(locator.Route)) + len(encodeSegment(locator.Key))
	if keyBytes > 2*1024 {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal replay locator exceeds key limit")
	}
	return nil
}

func encodeSegment(value string) string {
	return "~" + base64.RawURLEncoding.EncodeToString([]byte(value))
}

func validateTimestamp(field string, value time.Time) error {
	if value.IsZero() || value.Location() != time.UTC {
		return errs.Newf(errs.KindValidationFailed, "%s must be a non-zero UTC timestamp", field)
	}
	return nil
}
