package agentchannel

import (
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTaskStoreStatusPreservesNestedStorageUnavailability(t *testing.T) {
	storage := errs.New(errs.KindStorageUnavailable, "private backend detail")
	joined := errs.WrapJoined(
		errs.KindInternal,
		errs.New(errs.KindStateConflict, "operation failed"),
		storage,
	)
	result := taskStoreStatus(joined)
	if status.Code(result) != codes.Unavailable ||
		status.Convert(result).Message() != "agent task storage is unavailable" {
		t.Fatalf("nested storage failure mapped to %v", result)
	}
}
