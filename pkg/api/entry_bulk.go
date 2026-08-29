package api

const (
	MaximumBulkEntryCount        = 200
	MaximumBulkEntryPayloadBytes = 1 << 20
)

type EntryBulkItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type EntryBulkUpsertRequest struct {
	EnvironmentID string          `json:"environment_id"`
	Entries       []EntryBulkItem `json:"entries"`
	Exposure      []string        `json:"exposure"`
	Secret        bool            `json:"secret"`
}

type EntryBulkUpsertResult struct {
	TaskID  string  `json:"task_id"`
	Entries []Entry `json:"entries"`
}
