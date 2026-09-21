package composerender

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func composeIdentityTestID(kind ids.Kind, seed int64) string {
	return ids.NewAt(kind, time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC), seed)
}
