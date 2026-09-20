package app

import (
	"context"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: missing or unsupported authentication must fail before reaching
// the durable stage repository; a nil repository makes an accidental claim a
// test panic instead of a false success.
func TestBackingServiceAuthenticationRejectedBeforeDurableClaim(t *testing.T) {
	registerAdapters()
	service := &backingServiceCreationService{}
	for _, input := range []apiTypes.BackingServiceCreate{
		{Adapter: "postgres:16", Authentication: "none"},
		{Adapter: "postgres:16", Image: "postgres:latest"},
		{Adapter: "valkey:9"},
		{Adapter: "valkey:9", Authentication: " "},
		{Adapter: "valkey:9", Authentication: "invalid"},
		{Adapter: "custom"},
		{Adapter: "custom", Image: " "},
	} {
		_, err := service.CreateBackingService(context.Background(), input, "idempotency-key")
		kind, _ := errs.KindOf(err)
		if kind != errs.KindValidationFailed {
			t.Fatalf(
				"CreateBackingService(%s, authentication=%q, image=%q) error = %v",
				input.Adapter,
				input.Authentication,
				input.Image,
				err,
			)
		}
	}
}
