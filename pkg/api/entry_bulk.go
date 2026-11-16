package api

import "github.com/AlanD20/groundplane/internal/common/entrylimits"

const (
	MaximumBulkEntryCount        = entrylimits.MaximumBulkEntryCount
	MaximumBulkEntryPayloadBytes = 1 << 20
)

type EntryBulkItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type EntryBulkUpsertRequest struct {
	After         []string        `json:"after,omitempty" maxItems:"16" uniqueItems:"true"`
	EnvironmentID string          `json:"environment_id"`
	Entries       []EntryBulkItem `json:"entries"`
	Exposure      []string        `json:"exposure"`
	Secret        bool            `json:"secret"`
}
