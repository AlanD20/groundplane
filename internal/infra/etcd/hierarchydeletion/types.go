package hierarchydeletion

import (
	"time"
)

type HierarchyDeletionTargetKind string

const (
	HierarchyDeletionTargetTenant      HierarchyDeletionTargetKind = "tenant"
	HierarchyDeletionTargetProject     HierarchyDeletionTargetKind = "project"
	HierarchyDeletionTargetEnvironment HierarchyDeletionTargetKind = "environment"
	HierarchyDeletionTargetBacking     HierarchyDeletionTargetKind = "backing-service"
)

type HierarchyDeletionOperationKind string

const (
	HierarchyDeletionOperationTenant      HierarchyDeletionOperationKind = "tenant.delete"
	HierarchyDeletionOperationProject     HierarchyDeletionOperationKind = "project.delete"
	HierarchyDeletionOperationEnvironment HierarchyDeletionOperationKind = "environment.delete"
	HierarchyDeletionOperationBacking     HierarchyDeletionOperationKind = "backing.delete"
)

type HierarchyDeletionPhase string

const (
	HierarchyDeletionPlanning    HierarchyDeletionPhase = "planning"
	HierarchyDeletionExecuting   HierarchyDeletionPhase = "executing"
	HierarchyDeletionSummarizing HierarchyDeletionPhase = "summarizing"
	HierarchyDeletionFinalizing  HierarchyDeletionPhase = "finalizing"
	HierarchyDeletionRetained    HierarchyDeletionPhase = "retained"
)

type HierarchyDeletionDispatch string

const (
	HierarchyDeletionDispatchOpen     HierarchyDeletionDispatch = "open"
	HierarchyDeletionDispatchRetiring HierarchyDeletionDispatch = "retiring"
	HierarchyDeletionDispatchClosed   HierarchyDeletionDispatch = "closed"
)

type HierarchyDeletionActionKind string

const (
	HierarchyDeletionAttachDeprovision         HierarchyDeletionActionKind = "attach.deprovision"
	HierarchyDeletionAttachFinalize            HierarchyDeletionActionKind = "attach.finalize"
	HierarchyDeletionEnvironmentAgentCleanup   HierarchyDeletionActionKind = "environment.agent-cleanup"
	HierarchyDeletionServiceRemove             HierarchyDeletionActionKind = "service.remove"
	HierarchyDeletionEntryRemove               HierarchyDeletionActionKind = "entry.remove"
	HierarchyDeletionRouteRemove               HierarchyDeletionActionKind = "route.remove"
	HierarchyDeletionComponentRemove           HierarchyDeletionActionKind = "component.remove"
	HierarchyDeletionScriptRemove              HierarchyDeletionActionKind = "script.remove"
	HierarchyDeletionReleaseGroupRemove        HierarchyDeletionActionKind = "release-group.remove"
	HierarchyDeletionReleaseFinalize           HierarchyDeletionActionKind = "release.finalize"
	HierarchyDeletionBackupPolicyFinalize      HierarchyDeletionActionKind = "backup-policy.finalize"
	HierarchyDeletionKeyMaterialRemove         HierarchyDeletionActionKind = "key-material.remove"
	HierarchyDeletionMaterializationRemove     HierarchyDeletionActionKind = "materialization.remove"
	HierarchyDeletionZoneRemove                HierarchyDeletionActionKind = "zone.remove"
	HierarchyDeletionNetworkRemove             HierarchyDeletionActionKind = "network.remove"
	HierarchyDeletionReservationRelease        HierarchyDeletionActionKind = "reservation.release"
	HierarchyDeletionRecoveryPointRemove       HierarchyDeletionActionKind = "recovery-point.remove"
	HierarchyDeletionOrphanObjectRemove        HierarchyDeletionActionKind = "orphan-object.remove"
	HierarchyDeletionBackupSourceFinalize      HierarchyDeletionActionKind = "backup-source.finalize"
	HierarchyDeletionBackupScheduleFinalize    HierarchyDeletionActionKind = "backup-schedule.finalize"
	HierarchyDeletionBackupDueFinalize         HierarchyDeletionActionKind = "backup-due.finalize"
	HierarchyDeletionBackupRetentionFinalize   HierarchyDeletionActionKind = "backup-retention.finalize"
	HierarchyDeletionBackupRunDetach           HierarchyDeletionActionKind = "backup-run.detach"
	HierarchyDeletionBackupRestoreDetach       HierarchyDeletionActionKind = "backup-restore.detach"
	HierarchyDeletionBackupKeyRotationDetach   HierarchyDeletionActionKind = "backup-key-rotation.detach"
	HierarchyDeletionConnectorFinalize         HierarchyDeletionActionKind = "connector.finalize"
	HierarchyDeletionEnvironmentFinalize       HierarchyDeletionActionKind = "environment.finalize"
	HierarchyDeletionRunnerLocalRemove         HierarchyDeletionActionKind = "runner.local-remove"
	HierarchyDeletionProjectSecretRemove       HierarchyDeletionActionKind = "project-secret.remove"
	HierarchyDeletionBackingRuntimeReconstruct HierarchyDeletionActionKind = "backing.runtime-reconstruct"
	HierarchyDeletionBackingServiceFinalize    HierarchyDeletionActionKind = "backing-service.finalize"
	HierarchyDeletionProjectFinalize           HierarchyDeletionActionKind = "project.finalize"
	HierarchyDeletionTenantFinalize            HierarchyDeletionActionKind = "tenant.finalize"
)

type HierarchyDeletionActionTargetKind string

type HierarchyDeletionProcedureKind string

const (
	HierarchyDeletionProcedureAgent      HierarchyDeletionProcedureKind = "agent-work"
	HierarchyDeletionProcedureController HierarchyDeletionProcedureKind = "controller-finalizer"
)

type HierarchyCoordinationRecord struct {
	Schema        int                         `json:"schema"`
	TargetKind    HierarchyDeletionTargetKind `json:"target_kind"`
	TargetID      string                      `json:"target_id"`
	MutationEpoch int64                       `json:"mutation_epoch"`
}

type HierarchyDeletionWorkspace struct {
	Type     string `json:"type"`
	TenantID string `json:"tenant_id,omitempty"`
}

type HierarchyDeletionCheckpoint struct {
	NextOrdinal             int64  `json:"next_ordinal"`
	CompletedCount          int64  `json:"completed_count"`
	CompletedPrefixDigest   string `json:"completed_prefix_digest"`
	CompletionSummaryDigest string `json:"completion_summary_digest,omitempty"`
}

type HierarchyDeletionTerminal struct {
	Status                  string    `json:"status"`
	TaskID                  string    `json:"task_id"`
	CompletedAt             time.Time `json:"completed_at"`
	RetainUntil             time.Time `json:"retain_until"`
	CompletionSummaryDigest string    `json:"completion_summary_digest"`
}

type HierarchyDeletionTombstone struct {
	Schema           int                            `json:"schema"`
	TargetKind       HierarchyDeletionTargetKind    `json:"target_kind"`
	TargetID         string                         `json:"target_id"`
	TargetRevision   int64                          `json:"target_revision"`
	OperationKind    HierarchyDeletionOperationKind `json:"operation_kind"`
	OperationID      string                         `json:"operation_id"`
	DeletionEpoch    int64                          `json:"deletion_epoch"`
	CurrentTaskID    string                         `json:"current_task_id"`
	Workspace        HierarchyDeletionWorkspace     `json:"workspace"`
	SnapshotRevision int64                          `json:"snapshot_revision"`
	Phase            HierarchyDeletionPhase         `json:"phase"`
	PlanCount        *int64                         `json:"plan_count"`
	PlanDigest       *string                        `json:"plan_digest"`
	Checkpoint       HierarchyDeletionCheckpoint    `json:"checkpoint"`
	CreatedAt        time.Time                      `json:"created_at"`
	AttemptDeadline  time.Time                      `json:"attempt_deadline"`
	Terminal         *HierarchyDeletionTerminal     `json:"terminal"`
}

type HierarchyDeletionIntent struct {
	Schema           int                            `json:"schema"`
	OperationKind    HierarchyDeletionOperationKind `json:"operation_kind"`
	OperationID      string                         `json:"operation_id"`
	DeletionEpoch    int64                          `json:"deletion_epoch"`
	TargetKind       HierarchyDeletionTargetKind    `json:"target_kind"`
	TargetID         string                         `json:"target_id"`
	TargetRevision   int64                          `json:"target_revision"`
	Workspace        HierarchyDeletionWorkspace     `json:"workspace"`
	RootSlug         string                         `json:"root_slug"`
	SnapshotRevision int64                          `json:"snapshot_revision"`
	TimeoutSeconds   int64                          `json:"timeout_seconds"`
	CreatedAt        time.Time                      `json:"created_at"`
}

type HierarchyDeletionLock struct {
	Schema            int                         `json:"schema"`
	TargetKind        HierarchyDeletionTargetKind `json:"target_kind"`
	TargetID          string                      `json:"target_id"`
	ParentOperationID string                      `json:"parent_operation_id"`
	DeletionEpoch     int64                       `json:"deletion_epoch"`
	TombstoneKey      string                      `json:"tombstone_key"`
	CleanupFenceKey   string                      `json:"cleanup_fence_key"`
	CreatedAt         time.Time                   `json:"created_at"`
}

type HierarchyDeletionReplayLocator struct {
	Schema            int                            `json:"schema"`
	ParentOperationID string                         `json:"parent_operation_id"`
	OperationKind     HierarchyDeletionOperationKind `json:"operation_kind"`
	TargetKind        HierarchyDeletionTargetKind    `json:"target_kind"`
	TargetID          string                         `json:"target_id"`
	DeletionEpoch     int64                          `json:"deletion_epoch"`
	RootTaskID        string                         `json:"root_task_id"`
	CurrentTaskID     string                         `json:"current_task_id"`
	ResponseDigest    string                         `json:"response_digest"`
	TombstoneKey      string                         `json:"tombstone_key"`
	RetainUntil       *time.Time                     `json:"retain_until"`
}

type HierarchyDeletionCleanupFence struct {
	Schema            int                         `json:"schema"`
	ParentOperationID string                      `json:"parent_operation_id"`
	DeletionEpoch     int64                       `json:"deletion_epoch"`
	TargetKind        HierarchyDeletionTargetKind `json:"target_kind"`
	TargetID          string                      `json:"target_id"`
	PlanDigest        *string                     `json:"plan_digest"`
	Generation        int64                       `json:"generation"`
	Phase             HierarchyDeletionPhase      `json:"phase"`
	CurrentTaskID     string                      `json:"current_task_id"`
	Dispatch          HierarchyDeletionDispatch   `json:"dispatch"`
	UpdatedAt         time.Time                   `json:"updated_at"`
}

type HierarchyDeletionAgentProcedure struct {
	TypedProcedure string `json:"typed_procedure"`
	InputDigest    string `json:"input_digest"`
	TimeoutSeconds int64  `json:"timeout_seconds"`
}

type HierarchyDeletionControllerProcedure struct {
	Finalizer                   string `json:"finalizer"`
	FixedInputRevision          int64  `json:"fixed_input_revision"`
	CompareTemplateDigest       string `json:"compare_template_digest"`
	MutationTemplateDigest      string `json:"mutation_template_digest"`
	PostconditionTemplateDigest string `json:"postcondition_template_digest"`
}

type HierarchyDeletionAction struct {
	Schema               int                                   `json:"schema"`
	ParentOperationID    string                                `json:"parent_operation_id"`
	NodeID               string                                `json:"node_id"`
	Ordinal              int64                                 `json:"ordinal"`
	ActionKind           HierarchyDeletionActionKind           `json:"action_kind"`
	TargetKind           HierarchyDeletionActionTargetKind     `json:"target_kind"`
	TargetID             string                                `json:"target_id"`
	TargetRevision       int64                                 `json:"target_revision"`
	PrerequisiteOrdinals []int64                               `json:"prerequisite_ordinals"`
	ProcedureKind        HierarchyDeletionProcedureKind        `json:"procedure_kind"`
	AgentProcedure       *HierarchyDeletionAgentProcedure      `json:"agent_procedure,omitempty"`
	ControllerProcedure  *HierarchyDeletionControllerProcedure `json:"controller_procedure,omitempty"`
}

type HierarchyDeletionControllerCompletionProof struct {
	Finalizer                   string `json:"finalizer"`
	FixedInputRevision          int64  `json:"fixed_input_revision"`
	CompareTemplateDigest       string `json:"compare_template_digest"`
	MutationTemplateDigest      string `json:"mutation_template_digest"`
	PostconditionTemplateDigest string `json:"postcondition_template_digest"`
}

type HierarchyDeletionActionCompletion struct {
	Schema            int                                         `json:"schema"`
	ParentOperationID string                                      `json:"parent_operation_id"`
	DeletionEpoch     int64                                       `json:"deletion_epoch"`
	Ordinal           int64                                       `json:"ordinal"`
	ActionDigest      string                                      `json:"action_digest"`
	TargetKind        HierarchyDeletionActionTargetKind           `json:"target_kind"`
	TargetID          string                                      `json:"target_id"`
	TargetRevision    int64                                       `json:"target_revision"`
	Executor          HierarchyDeletionProcedureKind              `json:"executor"`
	ControllerProof   *HierarchyDeletionControllerCompletionProof `json:"controller_proof,omitempty"`
	CompletedAt       time.Time                                   `json:"completed_at"`
}
