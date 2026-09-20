package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"unicode/utf8"
)

func validateTaskRecord(record TaskRecord) error {
	if _, _, err := taskConfigurationCondition(record); err != nil {
		return err
	}
	if err := validateTaskEntryRuntime(record); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindTask, record.ID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindOperation, record.OperationID); err != nil {
		return err
	}
	if record.RetryOf != "" {
		if err := recordcodec.ValidateID(ids.KindTask, record.RetryOf); err != nil {
			return err
		}
		if record.RetryOf == record.ID {
			return errs.New(errs.KindValidationFailed, "task retry_of must name another task")
		}
	}
	if err := validateTaskOwner(record.Owner); err != nil {
		return err
	}
	if !validTaskActor(record.Actor) {
		return errs.New(errs.KindValidationFailed, "task actor is invalid")
	}
	if !validTaskExecutor(record.Executor) {
		return errs.New(errs.KindValidationFailed, "task executor is invalid")
	}
	if !validTaskType(record.Type) {
		return errs.New(errs.KindValidationFailed, "task type is not in the durable task catalog")
	}
	if record.Type == TaskBackupPrune && record.Actor != TaskActorSystem {
		return errs.New(errs.KindValidationFailed, "backup_prune task actor must be system")
	}
	if record.Target == "" || !utf8.ValidString(record.Target) {
		return errs.New(errs.KindValidationFailed, "task target is required and must be valid UTF-8")
	}
	if err := recordcodec.ValidateID(ids.KindPlan, record.PlanID); err != nil {
		return err
	}
	if !recordcodec.ValidSHA256(record.PlanHash) {
		return errs.New(errs.KindValidationFailed, "task plan hash must be a lowercase SHA-256 digest")
	}
	backupTask := record.Type == TaskBackup || record.Type == TaskBackupPrune
	if backupTask && (record.RenderGeneration != 0 || record.TimeoutSeconds != backupTaskTimeoutSeconds ||
		len(record.Params) != 0 || len(record.Materializations) != 0) {
		return errs.New(errs.KindValidationFailed, "backup task shape is invalid")
	}
	if !backupTask && record.RenderGeneration <= 0 {
		return errs.New(errs.KindValidationFailed, "task render_generation must be positive")
	}
	materializationEnvironment, hasMaterializationEnvironment, err := taskMaterializationEnvironment(record)
	if err != nil {
		return err
	}
	if record.idempotencyMarker != nil {
		if err := idempotencyrecord.ValidateIdempotencyLocator(*record.idempotencyMarker); err != nil {
			return errs.New(errs.KindInternal, "task idempotency marker locator is invalid")
		}
	}
	if record.TimeoutSeconds <= 0 {
		return errs.New(errs.KindValidationFailed, "task timeout_seconds must be positive")
	}
	if !validTaskStatus(record.Status) {
		return errs.New(errs.KindValidationFailed, "task status is invalid")
	}
	if record.TerminalAssignment != nil {
		identity := record.TerminalAssignment
		if record.Executor != TaskExecutorAgent || !isTerminalTaskStatus(record.Status) ||
			recordcodec.ValidateID(ids.KindAssignment, identity.AssignmentID) != nil ||
			recordcodec.ValidateID(ids.KindAgent, identity.AgentID) != nil || identity.AgentGeneration == 0 {
			return errs.New(errs.KindInternal, "task terminal assignment identity is invalid")
		}
	}
	if record.NextEventSequence == 0 ||
		uint64(record.EventCount) != min(record.NextEventSequence-1, MaximumTaskEvents) {
		return errs.New(errs.KindInternal, "task event summary is inconsistent")
	}
	if err := validateTaskEventCheckpoints(record); err != nil {
		return err
	}
	if err := recordcodec.ValidateTimestamp("task created_at", record.CreatedAt); err != nil {
		return err
	}
	if err := recordcodec.ValidateTimestamp("task updated_at", record.UpdatedAt); err != nil {
		return err
	}
	if record.UpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindInternal, "task updated_at precedes created_at")
	}
	if record.Status == TaskStatusPending && record.EventCount == 0 && !record.UpdatedAt.Equal(record.CreatedAt) {
		return errs.New(errs.KindInternal, "new pending task timestamps are inconsistent")
	}
	if err := validateTaskSteps(record.Steps); err != nil {
		return err
	}
	if err := executionplan.ValidateComponentActionStepIDs(record.ComponentActionStepIDs); err != nil {
		return err
	}
	if err := ValidateManagedComponentTeardownSources(record.ManagedComponentTeardownSources); err != nil {
		return err
	}
	for _, id := range record.ComponentActionStepIDs {
		found := false
		for _, step := range record.Steps {
			if step.ID == id {
				found = true
				break
			}
		}
		if !found {
			return errs.New(errs.KindValidationFailed, "Component action step is absent from Task")
		}
	}
	if err := validateTaskMaterializationReferences(
		record.Materializations,
		record.Steps,
		materializationEnvironment,
		hasMaterializationEnvironment,
		uint64(record.RenderGeneration),
	); err != nil {
		return err
	}
	if record.Result != nil {
		if !isTerminalTaskStatus(record.Status) {
			return errs.New(errs.KindInternal, "nonterminal task has a completion result")
		}
		if err := validateTaskResult(*record.Result, record.Steps, record.Status); err != nil {
			return err
		}
	}
	return validateTaskTimeline(record)
}

func validateTaskTimeline(record TaskRecord) error {
	if record.StartedAt != nil {
		if err := recordcodec.ValidateTimestamp("task started_at", *record.StartedAt); err != nil {
			return err
		}
		if record.StartedAt.Before(record.CreatedAt) {
			return errs.New(errs.KindInternal, "task started_at precedes created_at")
		}
		if record.UpdatedAt.Before(*record.StartedAt) {
			return errs.New(errs.KindInternal, "task updated_at precedes started_at")
		}
	}
	if isTerminalTaskStatus(record.Status) {
		if record.FinishedAt == nil || record.RetainUntil == nil {
			return errs.New(errs.KindInternal, "terminal task is missing retention timestamps")
		}
		if err := recordcodec.ValidateTimestamp("task finished_at", *record.FinishedAt); err != nil {
			return err
		}
		if err := recordcodec.ValidateTimestamp("task retain_until", *record.RetainUntil); err != nil {
			return err
		}
		if record.FinishedAt.Before(record.CreatedAt) ||
			(record.StartedAt != nil && record.FinishedAt.Before(*record.StartedAt)) {
			return errs.New(errs.KindInternal, "task finished_at precedes its lifecycle")
		}
		if !record.UpdatedAt.Equal(*record.FinishedAt) {
			return errs.New(errs.KindInternal, "terminal task updated_at does not equal finished_at")
		}
		if !record.RetainUntil.Equal(record.FinishedAt.Add(TaskRetention)) {
			return errs.New(errs.KindInternal, "task retention deadline is inconsistent")
		}
		return nil
	}
	if record.FinishedAt != nil || record.RetainUntil != nil {
		return errs.New(errs.KindInternal, "nonterminal task has terminal retention timestamps")
	}
	if record.Status == TaskStatusPending && record.StartedAt != nil {
		return errs.New(errs.KindInternal, "pending task has a started_at timestamp")
	}
	if record.Status == TaskStatusRunning && record.StartedAt == nil {
		return errs.New(errs.KindInternal, "running task is missing started_at")
	}
	return nil
}
