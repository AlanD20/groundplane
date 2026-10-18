// Package storagemaintenance defines output-free storage maintenance status.
package storagemaintenance

import "time"

type Pin struct {
	Kind     string `json:"kind"`
	OwnerID  string `json:"owner_id"`
	Revision int64  `json:"revision"`
}

type Status struct {
	Cleanup           *CleanupStatus `json:"cleanup,omitempty"`
	State             string         `json:"state"`
	ObservedAt        time.Time      `json:"observed_at"`
	BackendBytes      int64          `json:"backend_bytes"`
	InUseBytes        int64          `json:"in_use_bytes"`
	QuotaBytes        int64          `json:"quota_bytes"`
	HeadroomBytes     int64          `json:"headroom_bytes"`
	ReusableBytes     int64          `json:"reusable_bytes"`
	WALBytes          int64          `json:"wal_bytes"`
	CurrentValueBytes int64          `json:"current_value_bytes"`
	CurrentKeys       int64          `json:"current_keys"`
	CompactedThrough  int64          `json:"compacted_through"`
	ProtectedRevision int64          `json:"protected_revision,omitempty"`
	Blocker           *Pin           `json:"blocker,omitempty"`
	LastSuccessAt     *time.Time     `json:"last_success_at,omitempty"`
	LastFailureAt     *time.Time     `json:"last_failure_at,omitempty"`
	FailureCode       string         `json:"failure_code,omitempty"`
	LastDefragAt      *time.Time     `json:"last_defrag_at,omitempty"`
	ReclaimedBytes    int64          `json:"reclaimed_bytes"`
	DiagnosticBacklog int64          `json:"diagnostic_backlog"`
	TaskBacklog       int64          `json:"task_backlog"`
}

type CleanupStatus struct {
	ObservedAt         time.Time `json:"observed_at"`
	State              string    `json:"state"`
	IdempotencyRecords int       `json:"idempotency_records"`
	FailureCode        string    `json:"failure_code,omitempty"`
}
