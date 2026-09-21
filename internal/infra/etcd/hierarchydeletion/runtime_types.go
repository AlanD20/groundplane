package hierarchydeletion

import "time"

type HierarchyDeletionChildEntry struct {
	Schema                    int                                 `json:"schema"`
	ParentOperationID         string                              `json:"parent_operation_id"`
	ChildOperationID          string                              `json:"child_operation_id"`
	CurrentAttemptID          string                              `json:"current_attempt_id"`
	CurrentTaskID             string                              `json:"current_task_id"`
	CurrentTaskIdentityDigest string                              `json:"current_task_identity_digest"`
	DispatchState             HierarchyDeletionChildDispatchState `json:"dispatch_state"`
	Terminal                  *HierarchyDeletionAgentTerminal     `json:"terminal"`
	CheckpointDigest          string                              `json:"checkpoint_digest"`
	RetryInputDigest          string                              `json:"retry_input_digest"`
	RetrySharedOwner          HierarchyDeletionRetrySharedOwner   `json:"retry_shared_owner"`
}

type HierarchyDeletionSuccessor struct {
	Schema            int    `json:"schema"`
	ParentOperationID string `json:"parent_operation_id"`
	ChildOperationID  string `json:"child_operation_id"`
	ActionOrdinal     int64  `json:"action_ordinal"`
	AttemptID         string `json:"attempt_id"`
	TaskID            string `json:"task_id"`
}

type HierarchyDeletionChildDispatchState string

const (
	HierarchyDeletionChildAllocated       HierarchyDeletionChildDispatchState = "allocated"
	HierarchyDeletionChildDispatchVisible HierarchyDeletionChildDispatchState = "dispatch_visible"
	HierarchyDeletionChildTerminal        HierarchyDeletionChildDispatchState = "terminal"
)

type HierarchyDeletionRetrySharedOwner struct {
	Kind          string `json:"kind"`
	AttemptID     string `json:"attempt_id"`
	TaskID        string `json:"task_id,omitempty"`
	ReceiptDigest string `json:"receipt_digest,omitempty"`
}

type HierarchyDeletionTerminalAttemptReceipt struct {
	Schema             int                            `json:"schema"`
	ParentOperationID  string                         `json:"parent_operation_id"`
	ChildOperationID   string                         `json:"child_operation_id"`
	ActionOrdinal      int64                          `json:"action_ordinal"`
	AttemptID          string                         `json:"attempt_id"`
	TaskID             string                         `json:"task_id"`
	AssignmentID       string                         `json:"assignment_id"`
	AttemptGeneration  int64                          `json:"attempt_generation"`
	Terminal           HierarchyDeletionAgentTerminal `json:"terminal"`
	TerminalTaskDigest string                         `json:"terminal_task_digest"`
	ResultDigest       string                         `json:"result_digest,omitempty"`
	ErrorDigest        string                         `json:"error_digest,omitempty"`
	CheckpointDigest   string                         `json:"checkpoint_digest"`
	AgentAckRevision   int64                          `json:"agent_ack_revision"`
	PublishedAt        time.Time                      `json:"published_at"`
}

type HierarchyDeletionTerminalReceiptPointer struct {
	Schema                int    `json:"schema"`
	ParentOperationID     string `json:"parent_operation_id"`
	ChildOperationID      string `json:"child_operation_id"`
	CurrentAttemptID      string `json:"current_attempt_id"`
	CurrentReceiptDigest  string `json:"current_receipt_digest"`
	PreviousAttemptID     string `json:"previous_attempt_id,omitempty"`
	PreviousReceiptDigest string `json:"previous_receipt_digest,omitempty"`
}

type HierarchyDeletionConsumedAction string

const (
	HierarchyDeletionConsumedAdvance HierarchyDeletionConsumedAction = "advance"
	HierarchyDeletionConsumedRetry   HierarchyDeletionConsumedAction = "retry"
)

type HierarchyDeletionChildProgress struct {
	Schema             int                             `json:"schema"`
	ParentOperationID  string                          `json:"parent_operation_id"`
	ChildOperationID   string                          `json:"child_operation_id"`
	ActionOrdinal      int64                           `json:"action_ordinal"`
	TaskID             string                          `json:"task_id"`
	AssignmentID       string                          `json:"assignment_id"`
	AttemptGeneration  int64                           `json:"attempt_generation"`
	Terminal           HierarchyDeletionAgentTerminal  `json:"terminal"`
	TerminalTaskDigest string                          `json:"terminal_task_digest"`
	ResultDigest       string                          `json:"result_digest,omitempty"`
	ErrorDigest        string                          `json:"error_digest,omitempty"`
	CheckpointDigest   string                          `json:"checkpoint_digest"`
	ReceiptRevision    int64                           `json:"receipt_revision"`
	ReceiptDigest      string                          `json:"receipt_digest"`
	ConsumedAction     HierarchyDeletionConsumedAction `json:"consumed_action"`
}

type HierarchyDeletionScanCursor struct {
	Schema            int       `json:"schema"`
	ParentOperationID string    `json:"parent_operation_id"`
	TaskID            string    `json:"task_id"`
	NextOrdinal       int64     `json:"next_ordinal"`
	Count             int64     `json:"count"`
	OrderedSetDigest  string    `json:"ordered_set_digest"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type HierarchyDeletionPrunePhase string

const (
	HierarchyDeletionPruneReceipts    HierarchyDeletionPrunePhase = "receipts"
	HierarchyDeletionPruneProgress    HierarchyDeletionPrunePhase = "progress"
	HierarchyDeletionPruneChildren    HierarchyDeletionPrunePhase = "children"
	HierarchyDeletionPruneActions     HierarchyDeletionPrunePhase = "actions"
	HierarchyDeletionPruneCompletions HierarchyDeletionPrunePhase = "completions"
	HierarchyDeletionPruneFinal       HierarchyDeletionPrunePhase = "final"
)

type HierarchyDeletionPruneIntent struct {
	Schema            int                         `json:"schema"`
	ParentOperationID string                      `json:"parent_operation_id"`
	Phase             HierarchyDeletionPrunePhase `json:"phase"`
	NextOrdinal       int64                       `json:"next_ordinal"`
	RetainUntil       time.Time                   `json:"retain_until"`
	UpdatedAt         time.Time                   `json:"updated_at"`
}
