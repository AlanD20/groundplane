package app

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Generic journal fixtures carry no Script execution or source authority.
// Script tests opt into TaskScript and publish those required companions.

func taskJournalStepID() string {
	return ids.NewAt(ids.KindStep, taskJournalTime(), 5)
}

func taskJournalTime() time.Time {
	return time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
}
