package idempotency

import (
	errs "github.com/AlanD20/groundplane/pkg/errs"
	time "time"
)

func taskJournalTime() time.Time {
	return time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
}

func isKind(err error, want errs.Kind) bool {
	got, ok := errs.KindOf(err)
	return ok && got == want
}
