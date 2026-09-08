package app

import (
	"context"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: unsupported authentication input must fail before even reaching
// the durable stage repository; a nil repository makes an accidental claim a
// test panic instead of a false success.
func TestBackingServiceAuthenticationRejectedBeforeDurableClaim(t *testing.T) {
	registerAdapters()
	service := &backingServiceCreationService{}
	_, err := service.CreateBackingService(context.Background(), apiTypes.BackingServiceCreate{
		Adapter: "postgres:16", Authentication: "none",
	}, "idempotency-key")
	kind, _ := errs.KindOf(err)
	if kind != errs.KindValidationFailed {
		t.Fatalf("CreateBackingService(unsupported authentication) error = %v", err)
	}
}
