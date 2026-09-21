package entryvalues

import (
	sha256 "crypto/sha256"
	hex "encoding/hex"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	time "time"
)

func testPlainEntryValueGeneration() PlainGeneration {
	content := []byte("plain-entry-value")
	digest := sha256.Sum256(content)
	now := taskJournalTime()
	return PlainGeneration{
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 40),
		EntryID:       ids.NewAt(ids.KindEnvEntry, now, 41),
		GenerationID:  ids.NewAt(ids.KindConfig, now, 42),
		Content:       content, PlaintextSHA256: hex.EncodeToString(digest[:]), CreatedAt: now,
	}
}

func taskJournalTime() time.Time {
	return time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
}
