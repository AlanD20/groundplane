package api

// OnFailure is the shared service and release-group failure policy.
type OnFailure string

const (
	OnFailureSwitchBack  OnFailure = "switch_back"
	OnFailureLeaveActive OnFailure = "leave_active"
)

type ReleaseState string

const (
	ReleasePending          ReleaseState = "pending"
	ReleaseRunning          ReleaseState = "running"
	ReleaseCandidateHealthy ReleaseState = "candidate_healthy"
	ReleaseSwitching        ReleaseState = "switching"
	ReleaseServing          ReleaseState = "serving"
	ReleasePostHooks        ReleaseState = "post_hooks"
	ReleaseCompensating     ReleaseState = "compensating"
	ReleaseCompleted        ReleaseState = "completed"
	ReleaseFailed           ReleaseState = "failed"
	ReleaseTimedOut         ReleaseState = "timed_out"
	ReleaseAborted          ReleaseState = "aborted"
	ReleaseRecoveryRequired ReleaseState = "recovery_required"
	ReleaseRecovering       ReleaseState = "recovering"
)

type ReleaseSummary struct {
	ID                 string       `json:"id"`
	EnvironmentID      string       `json:"environment_id"`
	ServiceID          string       `json:"service_id"`
	OperationID        string       `json:"operation_id"`
	OperationKind      string       `json:"operation_kind"`
	GroupOperationID   string       `json:"group_operation_id,omitempty"`
	GroupMemberOrdinal uint32       `json:"group_member_ordinal,omitempty"`
	Image              string       `json:"image"`
	Tag                string       `json:"tag"`
	Digest             string       `json:"digest,omitempty"`
	Strategy           string       `json:"strategy"`
	Slot               string       `json:"slot,omitempty"`
	OnFailure          OnFailure    `json:"on_failure"`
	State              ReleaseState `json:"state"`
	CreatedAt          string       `json:"created_at"`
	CompletedAt        *string      `json:"completed_at,omitempty"`
	Serving            bool         `json:"serving"`
	CurrentSuccessful  bool         `json:"current_successful"`
}

type ReleaseAttempt struct {
	ID        string `json:"id"`
	TaskID    string `json:"task_id"`
	RetryOf   string `json:"retry_of,omitempty"`
	StartedAt string `json:"started_at"`
}

type ReleaseRollbackMaterial struct {
	Status    string  `json:"status"`
	Revision  uint64  `json:"revision"`
	ExpiredAt *string `json:"expired_at,omitempty"`
}

type ReleaseDetail struct {
	ReleaseSummary
	RollbackSourceReleaseID  string                   `json:"rollback_source_release_id,omitempty"`
	PriorServingReleaseID    string                   `json:"prior_serving_release_id,omitempty"`
	PriorSuccessfulReleaseID string                   `json:"prior_successful_release_id,omitempty"`
	RenderInputDigest        string                   `json:"render_input_digest"`
	OriginatingTaskID        string                   `json:"originating_task_id"`
	Attempts                 []ReleaseAttempt         `json:"attempts"`
	RollbackMaterial         *ReleaseRollbackMaterial `json:"rollback_material,omitempty"`
}

type ReleaseTaskAccepted struct {
	TaskID      string `json:"task_id"`
	OperationID string `json:"operation_id"`
	ReleaseID   string `json:"release_id"`
}

// ReleaseGroup coordinates multiple services under one task lock and
// one failure policy — never inferred, always explicit. See
// blueprint.md, "x-gp-release-group".
type ReleaseGroup struct {
	ID            string    `json:"id"`
	EnvironmentID string    `json:"environment_id"`
	Name          string    `json:"name"`
	ServiceIDs    []string  `json:"service_ids"`
	Order         []string  `json:"order"`
	Tag           string    `json:"tag,omitempty"`
	OnFailure     OnFailure `json:"on_failure"`
}

type ReleaseGroupAddRequest struct {
	EnvironmentID string    `json:"environment_id"`
	Name          string    `json:"name"`
	ServiceIDs    []string  `json:"service_ids"`
	Order         []string  `json:"order,omitempty"`
	Tag           string    `json:"tag,omitempty"`
	OnFailure     OnFailure `json:"on_failure,omitempty"`
}

type ReleaseGroupEditRequest struct {
	Name       *string                `json:"name,omitempty"`
	ServiceIDs *[]string              `json:"service_ids,omitempty"`
	Order      *[]string              `json:"order,omitempty"`
	Tag        OptionalNullableString `json:"tag,omitempty"`
	OnFailure  *OnFailure             `json:"on_failure,omitempty"`
}

type ReleaseGroupMutationAccepted struct {
	TaskID         string `json:"task_id"`
	ReleaseGroupID string `json:"release_group_id"`
}

type ReleaseGroupMemberRelease struct {
	ServiceID string `json:"service_id"`
	ReleaseID string `json:"release_id"`
}

type ReleaseGroupTaskAccepted struct {
	TaskID                  string                      `json:"task_id"`
	ReleaseGroupOperationID string                      `json:"release_group_operation_id"`
	Releases                []ReleaseGroupMemberRelease `json:"releases"`
}

// --- Ops action request bodies (every action returns a TaskAccepted) ---

type DeployRequest struct {
	Tag       string    `json:"tag,omitempty"`        // omitted => the service's current tag
	Strategy  string    `json:"strategy,omitempty"`   // omitted => the service's declared default
	OnFailure OnFailure `json:"on_failure,omitempty"` // omitted => "switch_back"
}

type RollbackRequest struct {
	Tag string `json:"tag,omitempty"` // omitted => the pre-selected previous tag
}

type ReleaseGroupDeployRequest struct {
	Tag string `json:"tag,omitempty"`
}
