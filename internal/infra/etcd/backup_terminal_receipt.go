package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backupTerminalReceiptPrefix = "/v1/runtime/backup-terminal-receipts/"

// BackupTerminalSourceOutcome is the immutable terminal outcome of one
// ordered Backup source. The full terminal run is bound separately by
// DomainDigest so the receipt does not duplicate potentially large snapshots.
type BackupTerminalSourceOutcome struct {
	Ordinal                uint32                                 `json:"ordinal"`
	SourceID               string                                 `json:"source_id"`
	Kind                   backupruntime.BackupRuntimeSourceKind  `json:"kind"`
	TargetID               string                                 `json:"target_id"`
	RecoveryPointID        string                                 `json:"recovery_point_id"`
	RecoveryPointCreatedAt time.Time                              `json:"recovery_point_created_at"`
	State                  backupruntime.BackupSourceAttemptState `json:"state"`
	Phase                  backupruntime.BackupSourceAttemptPhase `json:"phase"`
	SizeBytes              int64                                  `json:"size_bytes,omitempty"`
	SHA256                 string                                 `json:"sha256,omitempty"`
	FailureCode            backupruntime.BackupFailureCode        `json:"failure_code,omitempty"`
}

// BackupPruneTerminalPointOutcome is the immutable terminal result for one
// ordered Recovery Point in a Backup-prune Task.
type BackupPruneTerminalPointOutcome struct {
	Point     backupruntime.BackupRecoveryPointSnapshot `json:"point"`
	CreatedAt time.Time                                 `json:"created_at"`
	Outcome   BackupPruneTerminalOutcome                `json:"outcome"`
}

type BackupPruneTerminalOutcome string

const (
	BackupPruneTerminalRemoved  BackupPruneTerminalOutcome = "removed_verified_absent"
	BackupPruneTerminalRetained BackupPruneTerminalOutcome = "retained_pending_unassigned"
)

// BackupTerminalTaskEvidence explicitly binds the terminal Task contract.
// TaskDigest additionally covers every persisted Task field, including future
// fields that are not repeated in this projection.
type BackupTerminalTaskEvidence struct {
	TaskID             string                        `json:"task_id"`
	TaskType           TaskType                      `json:"task_type"`
	OperationID        string                        `json:"operation_id"`
	RetryOf            string                        `json:"retry_of,omitempty"`
	Owner              TaskOwner                     `json:"owner"`
	Actor              TaskActor                     `json:"actor"`
	Executor           TaskExecutor                  `json:"executor"`
	Target             string                        `json:"target"`
	PlanID             string                        `json:"plan_id"`
	PlanHash           string                        `json:"plan_hash"`
	Status             TaskStatus                    `json:"status"`
	ResultDigest       string                        `json:"result_digest"`
	TerminalAssignment *TaskTerminalAssignmentRecord `json:"terminal_assignment,omitempty"`
	CreatedAt          time.Time                     `json:"created_at"`
	UpdatedAt          time.Time                     `json:"updated_at"`
	StartedAt          *time.Time                    `json:"started_at,omitempty"`
	FinishedAt         time.Time                     `json:"finished_at"`
	RetainUntil        time.Time                     `json:"retain_until"`
	TaskDigest         string                        `json:"task_digest"`
}

// BackupTerminalReceiptRecord is compaction-independent terminal evidence for
// both Backup and Backup-prune Tasks. The receipt is immutable and is written
// in the exact transaction that writes the terminal Task and domain outcome.
type BackupTerminalReceiptRecord struct {
	Task                          BackupTerminalTaskEvidence        `json:"task"`
	PriorTaskRevision             int64                             `json:"prior_task_revision"`
	PriorEnvironmentEpochRevision int64                             `json:"prior_environment_epoch_revision"`
	EnvironmentEpochDigest        string                            `json:"environment_epoch_digest"`
	DomainDigest                  string                            `json:"domain_digest"`
	Sources                       []BackupTerminalSourceOutcome     `json:"sources,omitempty"`
	Points                        []BackupPruneTerminalPointOutcome `json:"points,omitempty"`
	ReceiptDigest                 string                            `json:"receipt_digest"`
}

type backupTerminalReceiptPlan struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	record     BackupTerminalReceiptRecord
}

func (plan *backupTerminalReceiptPlan) clear() {
	clearBackupRuntimeMutations(plan.mutations)
	plan.conditions = nil
	plan.mutations = nil
	plan.record = BackupTerminalReceiptRecord{}
}

func backupTerminalReceiptKey(taskID string) string {
	return backupTerminalReceiptPrefix + taskID
}

func encodeBackupTerminalReceiptRecord(record BackupTerminalReceiptRecord) ([]byte, error) {
	return backupruntime.EncodeBackupRuntimeRecord(
		"backup-terminal-receipt",
		record,
		validateBackupTerminalReceiptRecord,
	)
}

func decodeBackupTerminalReceiptRecord(value []byte) (BackupTerminalReceiptRecord, error) {
	return backupruntime.DecodeBackupRuntimeRecord(
		value,
		"backup-terminal-receipt",
		validateBackupTerminalReceiptRecord,
	)
}

func validateBackupTerminalReceiptRecord(record BackupTerminalReceiptRecord) error {
	if record.PriorTaskRevision <= 0 || record.PriorEnvironmentEpochRevision <= 0 ||
		!recordcodec.ValidSHA256(record.EnvironmentEpochDigest) ||
		!recordcodec.ValidSHA256(record.DomainDigest) ||
		!recordcodec.ValidSHA256(record.ReceiptDigest) || validateBackupTerminalTaskEvidence(record.Task) != nil {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt identity is invalid")
	}
	epochDigest, err := backupTerminalEnvironmentEpochDigest(record.Task.Owner.EnvironmentID)
	if err != nil || epochDigest != record.EnvironmentEpochDigest {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt Environment epoch is invalid")
	}
	switch record.Task.TaskType {
	case TaskBackup:
		if len(record.Sources) == 0 || len(record.Sources) > backuppolicy.MaximumBackupPolicySources ||
			len(record.Points) != 0 {
			return errs.New(errs.KindValidationFailed, "backup terminal receipt sources are invalid")
		}
		for index, source := range record.Sources {
			if source.Ordinal != uint32(index) ||
				recordcodec.ValidateID(ids.KindBackupSource, source.SourceID) != nil ||
				source.TargetID == "" ||
				recordcodec.ValidateID(ids.KindRecoveryPoint, source.RecoveryPointID) != nil ||
				!backupruntime.ValidBackupRuntimeInstant(source.RecoveryPointCreatedAt) ||
				source.RecoveryPointCreatedAt.After(record.Task.FinishedAt) ||
				!backupruntime.ValidBackupSourceAttemptState(source.State) ||
				!backupruntime.ValidBackupSourceAttemptPhase(source.Phase) ||
				!backupruntime.ValidBackupFailureCodeForAttempt(source.State, source.Phase, source.FailureCode) ||
				source.SizeBytes < 0 || (source.SHA256 != "" && !recordcodec.ValidSHA256(source.SHA256)) ||
				((source.SizeBytes > 0) != (source.SHA256 != "")) {
				return errs.New(errs.KindValidationFailed, "backup terminal receipt source is invalid")
			}
			switch source.Kind {
			case backupruntime.BackupRuntimeSourceAttach, backupruntime.BackupRuntimeSourceVolume, BackupRuntimeSourceConfig:
			default:
				return errs.New(errs.KindValidationFailed, "backup terminal receipt source kind is invalid")
			}
		}
	case TaskBackupPrune:
		if len(record.Sources) != 0 || len(record.Points) == 0 ||
			len(record.Points) > backupruntime.MaximumBackupPruneDispatchPoints {
			return errs.New(errs.KindValidationFailed, "backup terminal receipt points are invalid")
		}
		seen := make(map[string]struct{}, len(record.Points))
		for _, point := range record.Points {
			if backupruntime.ValidateBackupRecoveryPointSnapshot(point.Point) != nil ||
				point.Point.EnvironmentID != record.Task.Owner.EnvironmentID ||
				!backupruntime.ValidBackupRuntimeInstant(point.CreatedAt) ||
				point.CreatedAt.After(record.Task.FinishedAt) {
				return errs.New(errs.KindValidationFailed, "backup terminal receipt point is invalid")
			}
			if _, exists := seen[point.Point.ID]; exists {
				return errs.New(errs.KindValidationFailed, "backup terminal receipt points are duplicated")
			}
			seen[point.Point.ID] = struct{}{}
			if point.Outcome != BackupPruneTerminalRemoved &&
				point.Outcome != BackupPruneTerminalRetained {
				return errs.New(errs.KindValidationFailed, "backup terminal receipt outcome is invalid")
			}
		}
		domainDigest, err := backupTerminalDomainDigest(record.Points)
		if err != nil || domainDigest != record.DomainDigest {
			return errs.New(errs.KindValidationFailed, "backup terminal receipt point digest is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "backup terminal receipt Task type is invalid")
	}
	receiptDigest, err := backupTerminalReceiptDigest(record)
	if err != nil {
		return err
	}
	if receiptDigest != record.ReceiptDigest {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt digest is invalid")
	}
	return nil
}

func validateBackupTerminalTaskEvidence(evidence BackupTerminalTaskEvidence) error {
	if recordcodec.ValidateID(ids.KindTask, evidence.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindOperation, evidence.OperationID) != nil ||
		(evidence.RetryOf != "" && recordcodec.ValidateID(ids.KindTask, evidence.RetryOf) != nil) ||
		validateTaskOwner(evidence.Owner) != nil || evidence.Owner.EnvironmentID == "" ||
		!validTaskActor(evidence.Actor) || !validTaskExecutor(evidence.Executor) ||
		evidence.Executor != TaskExecutorAgent || evidence.Target != evidence.Owner.EnvironmentID ||
		recordcodec.ValidateID(ids.KindPlan, evidence.PlanID) != nil || !recordcodec.ValidSHA256(evidence.PlanHash) ||
		!isTerminalTaskStatus(evidence.Status) || !recordcodec.ValidSHA256(evidence.ResultDigest) ||
		!recordcodec.ValidSHA256(evidence.TaskDigest) || recordcodec.ValidateTimestamp("receipt created_at", evidence.CreatedAt) != nil ||
		recordcodec.ValidateTimestamp("receipt updated_at", evidence.UpdatedAt) != nil ||
		recordcodec.ValidateTimestamp("receipt finished_at", evidence.FinishedAt) != nil ||
		recordcodec.ValidateTimestamp("receipt retain_until", evidence.RetainUntil) != nil ||
		!evidence.UpdatedAt.Equal(evidence.FinishedAt) ||
		!evidence.RetainUntil.Equal(evidence.FinishedAt.Add(TaskRetention)) {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt Task evidence is invalid")
	}
	if evidence.StartedAt != nil && recordcodec.ValidateTimestamp("receipt started_at", *evidence.StartedAt) != nil {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt Task start is invalid")
	}
	if evidence.TerminalAssignment != nil {
		assignment := evidence.TerminalAssignment
		if evidence.StartedAt == nil || recordcodec.ValidateID(ids.KindAssignment, assignment.AssignmentID) != nil ||
			recordcodec.ValidateID(ids.KindAgent, assignment.AgentID) != nil || assignment.AgentGeneration == 0 {
			return errs.New(errs.KindValidationFailed, "backup terminal receipt assignment is invalid")
		}
	}
	if evidence.TaskType != TaskBackup && evidence.TaskType != TaskBackupPrune {
		return errs.New(errs.KindValidationFailed, "backup terminal receipt Task evidence type is invalid")
	}
	if evidence.TaskType == TaskBackupPrune && evidence.Actor != TaskActorSystem {
		return errs.New(errs.KindValidationFailed, "backup prune terminal receipt actor is invalid")
	}
	return nil
}

func backupTerminalResultDigest(result *TaskResultRecord) (string, error) {
	return backupTerminalCanonicalDigest("groundplane.backup.terminal.result.v1\x00", result)
}

func backupTerminalTaskDigest(task TaskRecord) (string, error) {
	value, err := encodeTaskRecord(task)
	if err != nil {
		return "", err
	}
	defer clear(value)
	digest := sha256.Sum256(append([]byte("groundplane.backup.terminal.task.v1\x00"), value...))
	return hex.EncodeToString(digest[:]), nil
}

func backupTerminalDomainDigest(value any) (string, error) {
	return backupTerminalCanonicalDigest("groundplane.backup.terminal.domain.v1\x00", value)
}

func backupTerminalEnvironmentEpochDigest(environmentID string) (string, error) {
	value, err := backupruntime.EncodeEnvironmentMutationEpochRecord(backupruntime.EnvironmentMutationEpochRecord{
		EnvironmentID: environmentID,
	})
	if err != nil {
		return "", err
	}
	defer clear(value)
	digest := sha256.Sum256(append([]byte("groundplane.backup.terminal.epoch.v1\x00"), value...))
	return hex.EncodeToString(digest[:]), nil
}

func backupTerminalCanonicalDigest(domain string, value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(append([]byte(domain), encoded...))
	return hex.EncodeToString(digest[:]), nil
}

func backupTerminalReceiptDigest(record BackupTerminalReceiptRecord) (string, error) {
	record.ReceiptDigest = ""
	encoded, err := json.Marshal(record)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(append([]byte("groundplane.backup.terminal.receipt.v1\x00"), encoded...))
	return hex.EncodeToString(digest[:]), nil
}

func backupTerminalTaskEvidence(task TaskRecord) (BackupTerminalTaskEvidence, error) {
	if task.FinishedAt == nil || task.RetainUntil == nil {
		return BackupTerminalTaskEvidence{}, errs.New(
			errs.KindValidationFailed,
			"backup terminal receipt requires terminal Task timestamps",
		)
	}
	resultDigest, err := backupTerminalResultDigest(task.Result)
	if err != nil {
		return BackupTerminalTaskEvidence{}, err
	}
	taskDigest, err := backupTerminalTaskDigest(task)
	if err != nil {
		return BackupTerminalTaskEvidence{}, err
	}
	evidence := BackupTerminalTaskEvidence{
		TaskID: task.ID, TaskType: task.Type, OperationID: task.OperationID, RetryOf: task.RetryOf,
		Owner: task.Owner, Actor: task.Actor, Executor: task.Executor, Target: task.Target,
		PlanID: task.PlanID, PlanHash: task.PlanHash, Status: task.Status,
		ResultDigest: resultDigest, TerminalAssignment: cloneTaskTerminalAssignment(task.TerminalAssignment),
		CreatedAt: task.CreatedAt, UpdatedAt: task.UpdatedAt,
		FinishedAt: *task.FinishedAt, RetainUntil: *task.RetainUntil, TaskDigest: taskDigest,
	}
	if task.StartedAt != nil {
		startedAt := *task.StartedAt
		evidence.StartedAt = &startedAt
	}
	if err := validateBackupTerminalTaskEvidence(evidence); err != nil {
		return BackupTerminalTaskEvidence{}, err
	}
	return evidence, nil
}

func prepareBackupRunTerminalReceipt(
	current etcdstore.Versioned[TaskRecord],
	terminal TaskRecord,
	run backupruntime.BackupRunRecord,
) (backupTerminalReceiptPlan, error) {
	if current.Revision <= 0 || current.Record.Type != TaskBackup || terminal.Type != TaskBackup ||
		current.Record.ID != terminal.ID || !isTerminalTaskStatus(terminal.Status) ||
		validateBackupRunTaskBinding(terminal, run) != nil || !terminalBackupRunState(run.State) ||
		terminal.FinishedAt == nil || !run.UpdatedAt.Equal(*terminal.FinishedAt) {
		return backupTerminalReceiptPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup terminal receipt binding is invalid",
		)
	}
	wantState, err := backupRunStateForTaskStatus(terminal.Status)
	if err != nil || run.State != wantState {
		return backupTerminalReceiptPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup terminal receipt outcome is invalid",
		)
	}
	evidence, err := backupTerminalTaskEvidence(terminal)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	runValue, err := backupruntime.EncodeBackupRunRecord(run)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	defer clear(runValue)
	domainDigest := sha256.Sum256(append([]byte("groundplane.backup.terminal.run.v1\x00"), runValue...))
	receipt := BackupTerminalReceiptRecord{
		Task: evidence, PriorTaskRevision: current.Revision,
		DomainDigest: hex.EncodeToString(domainDigest[:]),
		Sources:      backupTerminalRunOutcomes(run),
	}
	return prepareBackupTerminalReceiptPlan(receipt)
}

func backupTerminalRunOutcomes(run backupruntime.BackupRunRecord) []BackupTerminalSourceOutcome {
	outcomes := make([]BackupTerminalSourceOutcome, len(run.Sources))
	for index, source := range run.Sources {
		outcomes[index] = BackupTerminalSourceOutcome{
			Ordinal: source.Ordinal, SourceID: source.SourceID, Kind: source.Kind,
			TargetID: source.TargetID, RecoveryPointID: source.RecoveryPointID,
			RecoveryPointCreatedAt: source.RecoveryPointCreatedAt,
			State:                  source.State, Phase: source.Phase, SizeBytes: source.SizeBytes,
			SHA256: source.SHA256, FailureCode: source.FailureCode,
		}
	}
	return outcomes
}

func prepareBackupPruneTerminalReceipt(
	current etcdstore.Versioned[TaskRecord],
	terminal TaskRecord,
	dispatch backupruntime.BackupRecoveryPointPruneDispatchRecord,
	prunes []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
) (backupTerminalReceiptPlan, error) {
	if current.Revision <= 0 || current.Record.Type != TaskBackupPrune ||
		terminal.Type != TaskBackupPrune || !isTerminalTaskStatus(terminal.Status) ||
		terminal.FinishedAt == nil || validateBackupPruneTaskBinding(terminal, dispatch) != nil ||
		len(prunes) != len(dispatch.RecoveryPointIDs) || terminal.ID != current.Record.ID {
		return backupTerminalReceiptPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup prune terminal receipt binding is invalid",
		)
	}
	evidence, err := backupTerminalTaskEvidence(terminal)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	receipt := BackupTerminalReceiptRecord{
		Task: evidence, PriorTaskRevision: current.Revision,
		Points: make([]BackupPruneTerminalPointOutcome, len(prunes)),
	}
	for index, prune := range prunes {
		if prune.Revision <= 0 || prune.Record.Point.ID != dispatch.RecoveryPointIDs[index] ||
			prune.Record.OperationID != dispatch.OperationID || prune.Record.TaskID != dispatch.TaskID ||
			prune.Record.Point.EnvironmentID != dispatch.EnvironmentID ||
			prune.Record.CreatedAt.After(*terminal.FinishedAt) {
			return backupTerminalReceiptPlan{}, errs.New(
				errs.KindValidationFailed,
				"backup prune terminal receipt point binding is invalid",
			)
		}
		outcome := BackupPruneTerminalRetained
		if prune.Record.State == backupruntime.BackupPruneVerifiedAbsent {
			outcome = BackupPruneTerminalRemoved
		} else if prune.Record.State != backupruntime.BackupPruneAssigned || terminal.Status == TaskStatusCompleted {
			return backupTerminalReceiptPlan{}, errs.New(
				errs.KindValidationFailed,
				"backup prune terminal receipt outcome is invalid",
			)
		}
		receipt.Points[index] = BackupPruneTerminalPointOutcome{
			Point: prune.Record.Point, CreatedAt: prune.Record.CreatedAt, Outcome: outcome,
		}
	}
	receipt.DomainDigest, err = backupTerminalDomainDigest(receipt.Points)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	return prepareBackupTerminalReceiptPlan(receipt)
}

func prepareBackupTerminalReceiptPlan(
	receipt BackupTerminalReceiptRecord,
) (backupTerminalReceiptPlan, error) {
	epochDigest, err := backupTerminalEnvironmentEpochDigest(receipt.Task.Owner.EnvironmentID)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	// The exact prior epoch ModRevision is carried by the domain plan and is
	// bound by composeBackupTerminalTransaction before persistence.
	receipt.PriorEnvironmentEpochRevision = 1
	receipt.EnvironmentEpochDigest = epochDigest
	receipt.ReceiptDigest, err = backupTerminalReceiptDigest(receipt)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	value, err := encodeBackupTerminalReceiptRecord(receipt)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	key := backupTerminalReceiptKey(receipt.Task.TaskID)
	// The terminal Task ModRevision compare serializes every valid receipt
	// writer. A second receipt compare would spend the closed prune transaction's
	// final operation without strengthening that fence.
	return backupTerminalReceiptPlan{
		mutations: []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}},
		record:    receipt,
	}, nil
}

func composeBackupRunTerminalTransaction(
	taskPlan backupTaskTerminalPlan,
	runPlan backupRunPublicationPlan,
	receiptPlan backupTerminalReceiptPlan,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	return composeBackupTerminalTransaction(
		taskPlan.conditions, taskPlan.mutations,
		runPlan.conditions, runPlan.mutations,
		receiptPlan,
	)
}

func composeBackupPruneTerminalTransaction(
	taskPlan backupTaskTerminalPlan,
	prunePlan backupPruneTransactionPlan,
	receiptPlan backupTerminalReceiptPlan,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	return composeBackupTerminalTransaction(
		taskPlan.conditions, taskPlan.mutations,
		prunePlan.conditions, prunePlan.mutations,
		receiptPlan,
	)
}

func composeBackupTerminalTransaction(
	taskConditions []etcdstore.Condition,
	taskMutations []etcdstore.Mutation,
	domainConditions []etcdstore.Condition,
	domainMutations []etcdstore.Mutation,
	receiptPlan backupTerminalReceiptPlan,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	boundReceipt, err := bindBackupTerminalReceiptEpoch(receiptPlan, domainConditions)
	if err != nil {
		return nil, nil, err
	}
	defer boundReceipt.clear()
	conditions := append(append([]etcdstore.Condition(nil), taskConditions...), domainConditions...)
	conditions = append(conditions, boundReceipt.conditions...)
	mutations := make([]etcdstore.Mutation, 0, len(taskMutations)+len(domainMutations)+len(boundReceipt.mutations))
	for _, plan := range [][]etcdstore.Mutation{taskMutations, domainMutations, boundReceipt.mutations} {
		for _, mutation := range plan {
			copyOfMutation := mutation
			copyOfMutation.Value = append([]byte(nil), mutation.Value...)
			mutations = append(mutations, copyOfMutation)
		}
	}
	if err := validateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		clearBackupRuntimeMutations(mutations)
		return nil, nil, err
	}
	return conditions, mutations, nil
}

func bindBackupTerminalReceiptEpoch(
	plan backupTerminalReceiptPlan,
	domainConditions []etcdstore.Condition,
) (backupTerminalReceiptPlan, error) {
	wantKey := hierarchyrecord.EnvironmentMutationEpochKey(plan.record.Task.Owner.EnvironmentID)
	var revision int64
	for _, condition := range domainConditions {
		if condition.Key != wantKey {
			continue
		}
		if revision != 0 || condition.ModRevision <= 0 {
			return backupTerminalReceiptPlan{}, errs.New(
				errs.KindInternal,
				"backup terminal receipt epoch fence is ambiguous",
			)
		}
		revision = condition.ModRevision
	}
	if revision <= 0 {
		return backupTerminalReceiptPlan{}, errs.New(
			errs.KindInternal,
			"backup terminal receipt epoch fence is missing",
		)
	}
	record := plan.record
	record.PriorEnvironmentEpochRevision = revision
	receiptDigest, err := backupTerminalReceiptDigest(record)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	record.ReceiptDigest = receiptDigest
	value, err := encodeBackupTerminalReceiptRecord(record)
	if err != nil {
		return backupTerminalReceiptPlan{}, err
	}
	return backupTerminalReceiptPlan{
		mutations: []etcdstore.Mutation{{
			Type: etcdstore.MutationPut, Key: backupTerminalReceiptKey(record.Task.TaskID), Value: value,
		}},
		record: record,
	}, nil
}

func (repository *TaskRepository) validateBackupTerminalReceiptReplay(
	ctx context.Context,
	task etcdstore.Versioned[TaskRecord],
) error {
	if task.Revision <= 0 || (task.Record.Type != TaskBackup && task.Record.Type != TaskBackupPrune) ||
		!isTerminalTaskStatus(task.Record.Status) {
		return errs.New(errs.KindInternal, "terminal backup Task is invalid")
	}
	keys := []string{taskKey(task.Record.ID), backupTerminalReceiptKey(task.Record.ID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return errs.New(errs.KindInternal, "backup terminal receipt read is incomplete")
	}
	if read.Values[0] == nil || read.Values[0].ModRevision != task.Revision {
		return errs.New(errs.KindStateConflict, "terminal backup Task changed")
	}
	if read.Values[1] == nil || read.Values[1].ModRevision != task.Revision {
		return errs.New(errs.KindInternal, "backup terminal receipt is not atomic with its Task")
	}
	storedTask, err := decodeTaskRecord(read.Values[0].Value)
	if err != nil {
		return err
	}
	storedDigest, err := backupTerminalTaskDigest(storedTask)
	if err != nil {
		return err
	}
	callerDigest, err := backupTerminalTaskDigest(task.Record)
	if err != nil || callerDigest != storedDigest {
		return errs.New(errs.KindStateConflict, "terminal backup Task changed")
	}
	receipt, err := decodeBackupTerminalReceiptRecord(read.Values[1].Value)
	if err != nil {
		return err
	}
	if err := validateBackupTerminalReceiptTaskBinding(storedTask, receipt); err != nil ||
		receipt.PriorTaskRevision >= task.Revision ||
		receipt.PriorEnvironmentEpochRevision >= task.Revision {
		return errs.New(errs.KindInternal, "backup terminal receipt binding is invalid")
	}
	return repository.validateCurrentBackupTerminalAuthority(
		ctx,
		task.Record,
		task.Revision,
		receipt,
	)
}

func validateBackupTerminalReceiptTaskBinding(
	task TaskRecord,
	receipt BackupTerminalReceiptRecord,
) error {
	evidence, err := backupTerminalTaskEvidence(task)
	if err != nil {
		return err
	}
	left, err := json.Marshal(evidence)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	right, err := json.Marshal(receipt.Task)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if string(left) != string(right) {
		return errs.New(errs.KindStateConflict, "backup terminal receipt Task binding is invalid")
	}
	return nil
}

func (repository *TaskRepository) validateCurrentBackupTerminalAuthority(
	ctx context.Context,
	task TaskRecord,
	terminalRevision int64,
	receipt BackupTerminalReceiptRecord,
) error {
	environmentID := receipt.Task.Owner.EnvironmentID
	keys := []string{
		hierarchyrecord.EnvironmentKey(environmentID),
		hierarchyrecord.EnvironmentMutationEpochKey(environmentID),
		hierarchyrecord.EnvironmentOperationLockKey(environmentID),
		deletionTombstoneKey(string(DeletionTargetEnvironment), environmentID),
		backupruntime.BackupRecoveryPointPruneDispatchKey(receipt.Task.TaskID),
	}
	runIndex, membershipIndex, exclusionsStart := -1, -1, -1
	var exclusionKeys []string
	if receipt.Task.TaskType == TaskBackup {
		membershipKey, err := backupruntime.BackupRunEnvironmentIndexKey(environmentID, receipt.Task.TaskID)
		if err != nil {
			return err
		}
		exclusionKeys, err = backupTerminalExclusionKeys(receipt.Sources)
		if err != nil {
			return err
		}
		runIndex = len(keys)
		keys = append(keys, backupruntime.BackupRunKey(receipt.Task.TaskID))
		membershipIndex = len(keys)
		keys = append(keys, membershipKey)
		exclusionsStart = len(keys)
		keys = append(keys, exclusionKeys...)
	}
	authorityEnd := len(keys)
	pointsStart := len(keys)
	for _, outcome := range receipt.Points {
		pointKeys, err := backupPruneAuthorityKeys(outcome.Point)
		if err != nil {
			return err
		}
		keys = append(keys, pointKeys...)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return errs.New(errs.KindInternal, "backup terminal authority read is incomplete")
	}
	defer clearKeyValues(read.Values)
	ownerPresent, successorLock, deletionOwner, err := repository.validateBackupTerminalOwnerSnapshot(
		ctx,
		read.ReadRevision,
		terminalRevision,
		receipt,
		read.Values[:authorityEnd],
	)
	if err != nil {
		return err
	}
	if receipt.Task.TaskType == TaskBackup {
		runValue, membershipValue := read.Values[runIndex], read.Values[membershipIndex]
		exclusionValues := read.Values[exclusionsStart:authorityEnd]
		if !ownerPresent {
			return nil
		}
		if runValue == nil || membershipValue == nil {
			if runValue != nil || membershipValue != nil || !deletionOwner ||
				!allBackupRuntimeValuesAbsent(exclusionValues) {
				return errs.New(errs.KindStateConflict, "terminal backup owner authority is torn")
			}
			return nil
		}
		if membershipValue.Key != keys[membershipIndex] || membershipValue.Version != 1 ||
			membershipValue.ModRevision > terminalRevision ||
			string(membershipValue.Value) != receipt.Task.TaskID {
			return errs.New(errs.KindStateConflict, "terminal backup run membership changed")
		}
		run, err := backupruntime.DecodeBackupRunRecord(runValue.Value)
		if err != nil {
			return err
		}
		if runValue.ModRevision != terminalRevision {
			return errs.New(errs.KindStateConflict, "terminal backup run binding changed")
		}
		if validateBackupRunTaskBinding(task, run) != nil {
			return errs.New(errs.KindInternal, "same-revision terminal backup run binding is invalid")
		}
		value, err := backupruntime.EncodeBackupRunRecord(run)
		if err != nil {
			return err
		}
		defer clear(value)
		digest := sha256.Sum256(append([]byte("groundplane.backup.terminal.run.v1\x00"), value...))
		if hex.EncodeToString(digest[:]) != receipt.DomainDigest {
			return errs.New(errs.KindInternal, "same-revision terminal backup run digest is invalid")
		}
		outcomes := backupTerminalRunOutcomes(run)
		if !slices.Equal(outcomes, receipt.Sources) {
			return errs.New(errs.KindInternal, "same-revision terminal backup source outcomes are invalid")
		}
		for index, exclusionValue := range exclusionValues {
			if exclusionValue == nil {
				continue
			}
			exclusion, err := backupruntime.DecodeBackupSourceTargetExclusionRecord(exclusionValue.Value)
			if err != nil {
				return err
			}
			exclusionKey, keyErr := backupruntime.BackupSourceTargetExclusionKey(
				exclusion.TargetKind,
				exclusion.TargetID,
			)
			if keyErr != nil || exclusion.EnvironmentID != environmentID ||
				exclusion.TaskID == receipt.Task.TaskID || successorLock == nil ||
				exclusion.OperationID != successorLock.OperationID ||
				exclusion.TaskID != successorLock.TaskID ||
				exclusion.OperationKind != successorLock.Kind ||
				exclusionValue.ModRevision != read.Values[2].ModRevision ||
				exclusionValue.Key != exclusionKeys[index] || exclusionKey != exclusionValue.Key {
				return errs.New(errs.KindStateConflict, "terminal backup exclusion authority changed")
			}
		}
		return nil
	}
	for index, outcome := range receipt.Points {
		values := read.Values[pointsStart+index*5 : pointsStart+index*5+5]
		if outcome.Outcome == BackupPruneTerminalRemoved {
			if !allBackupRuntimeValuesAbsent(values) {
				return errs.New(
					errs.KindStateConflict,
					"removed terminal backup prune authority was reconstructed",
				)
			}
			continue
		}
		if values[0] == nil {
			if !allBackupRuntimeValuesAbsent(values) {
				return errs.New(
					errs.KindStateConflict,
					"terminal backup prune point authority is torn",
				)
			}
			continue
		}
		if !ownerPresent {
			return errs.New(errs.KindStateConflict, "deleted backup owner retained point authority")
		}
		prune, err := backupruntime.DecodeBackupRecoveryPointPruneRecord(values[0].Value)
		if err != nil {
			return err
		}
		if values[0].ModRevision == terminalRevision {
			if outcome.Outcome != BackupPruneTerminalRetained || prune.Point != outcome.Point ||
				prune.OperationID != receipt.Task.OperationID || prune.State != backupruntime.BackupPrunePending ||
				prune.TaskID != "" || prune.CreatedAt != outcome.CreatedAt ||
				!prune.UpdatedAt.Equal(receipt.Task.FinishedAt) {
				return errs.New(errs.KindInternal, "same-revision backup prune outcome is invalid")
			}
			if err := validatePendingBackupPruneAuthority(
				values,
				etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
					Record:   prune,
					Revision: values[0].ModRevision,
				},
			); err != nil {
				return errs.New(errs.KindInternal, "same-revision backup prune authority is invalid")
			}
			continue
		}
		if values[0].ModRevision < terminalRevision || prune.TaskID == receipt.Task.TaskID {
			return errs.New(errs.KindStateConflict, "stale terminal backup prune owner remains")
		}
		if prune.Point != outcome.Point || prune.CreatedAt != outcome.CreatedAt {
			return errs.New(errs.KindStateConflict, "terminal backup prune successor changed")
		}
		if err := validatePendingBackupPruneAuthority(
			values,
			etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
				Record: prune, Revision: values[0].ModRevision,
			},
		); err != nil {
			return err
		}
		if prune.TaskID == "" {
			if prune.State != backupruntime.BackupPrunePending {
				return errs.New(errs.KindStateConflict, "terminal backup prune successor changed")
			}
			continue
		}
		if successorLock == nil || successorLock.Kind != backupruntime.BackupOperationPrune ||
			successorLock.OperationID != prune.OperationID || successorLock.TaskID != prune.TaskID {
			return errs.New(errs.KindStateConflict, "terminal backup prune successor ownership changed")
		}
		dispatchRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys:     []string{backupruntime.BackupRecoveryPointPruneDispatchKey(prune.TaskID)},
			Revision: read.ReadRevision,
		})
		if err != nil {
			return err
		}
		if dispatchRead == nil || dispatchRead.ReadRevision != read.ReadRevision ||
			len(dispatchRead.Values) != 1 || dispatchRead.Values[0] == nil {
			if dispatchRead != nil {
				clearKeyValues(dispatchRead.Values)
			}
			return errs.New(errs.KindStateConflict, "terminal backup prune successor dispatch is missing")
		}
		dispatchRevision := dispatchRead.Values[0].ModRevision
		dispatch, err := backupruntime.DecodeBackupRecoveryPointPruneDispatchRecord(dispatchRead.Values[0].Value)
		clearKeyValues(dispatchRead.Values)
		if err != nil {
			return err
		}
		if dispatch.EnvironmentID != environmentID || dispatch.OperationID != prune.OperationID ||
			dispatch.TaskID != prune.TaskID || !dispatch.CreatedAt.Equal(successorLock.CreatedAt) ||
			dispatchRevision != values[0].ModRevision ||
			values[0].ModRevision != read.Values[2].ModRevision ||
			!slices.Contains(dispatch.RecoveryPointIDs, prune.Point.ID) {
			return errs.New(errs.KindStateConflict, "terminal backup prune successor dispatch changed")
		}
	}
	return nil
}

func backupTerminalExclusionKeys(
	sources []BackupTerminalSourceOutcome,
) ([]string, error) {
	byKey := make(map[string]struct{})
	for _, source := range sources {
		var kind backupruntime.BackupSourceTargetKind
		switch source.Kind {
		case BackupRuntimeSourceAttach:
			kind = backupruntime.BackupSourceTargetAttach
		case BackupRuntimeSourceVolume:
			kind = backupruntime.BackupSourceTargetVolume
		case BackupRuntimeSourceConfig:
			continue
		default:
			return nil, errs.New(errs.KindInternal, "backup terminal source kind is invalid")
		}
		key, err := backupruntime.BackupSourceTargetExclusionKey(kind, source.TargetID)
		if err != nil {
			return nil, err
		}
		byKey[key] = struct{}{}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys, nil
}

func (repository *TaskRepository) validateBackupTerminalOwnerSnapshot(
	ctx context.Context,
	readRevision int64,
	terminalRevision int64,
	receipt BackupTerminalReceiptRecord,
	values []*etcdstore.KeyValue,
) (bool, *backupruntime.BackupOperationLockRecord, bool, error) {
	if len(values) < 5 {
		return false, nil, false, errs.New(errs.KindInternal, "backup terminal owner snapshot is incomplete")
	}
	environmentValue, epochValue, lockValue, tombstoneValue, dispatchValue :=
		values[0], values[1], values[2], values[3], values[4]
	if environmentValue == nil {
		if !allBackupRuntimeValuesAbsent(values[1:]) {
			return false, nil, false, errs.New(
				errs.KindStateConflict,
				"deleted backup owner retained terminal authority",
			)
		}
		return false, nil, false, nil
	}
	environment, err := hierarchyrecord.DecodeEnvironment(environmentValue.Value)
	if err != nil {
		return false, nil, false, err
	}
	if environment.ID != receipt.Task.Owner.EnvironmentID {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup Environment binding changed")
	}
	if epochValue == nil || epochValue.ModRevision < terminalRevision {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup Environment epoch changed")
	}
	epoch, err := backupruntime.DecodeEnvironmentMutationEpochRecord(epochValue.Value)
	if err != nil {
		return false, nil, false, err
	}
	if epoch.EnvironmentID != environment.ID {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup Environment epoch changed")
	}
	if dispatchValue != nil {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup prune dispatch remains")
	}
	if lockValue == nil {
		if tombstoneValue != nil {
			return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup deletion authority is torn")
		}
		return true, nil, false, nil
	}
	lock, err := backupruntime.DecodeBackupOperationLockRecord(lockValue.Value)
	if err != nil {
		return false, nil, false, err
	}
	if lock.EnvironmentID != environment.ID || lockValue.ModRevision <= terminalRevision ||
		lock.TaskID == receipt.Task.TaskID {
		return false, nil, false, errs.New(errs.KindStateConflict, "stale terminal backup lock remains")
	}
	if lock.Kind != backupruntime.BackupOperationDeletion {
		if tombstoneValue != nil {
			return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup deletion authority is torn")
		}
		return true, &lock, false, nil
	}
	if tombstoneValue == nil {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup deletion authority is torn")
	}
	tombstone, err := decodeDeletionTombstone(tombstoneValue.Value)
	if err != nil {
		return false, nil, false, err
	}
	if tombstone.TargetKind != DeletionTargetEnvironment || tombstone.TargetID != environment.ID ||
		tombstone.TargetRevision != environmentValue.ModRevision || tombstone.TaskID != lock.TaskID {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup deletion authority changed")
	}
	intentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentDeletionIntentKey(lock.OperationID)}, Revision: readRevision,
	})
	if err != nil {
		return false, nil, false, err
	}
	if intentRead == nil || intentRead.ReadRevision != readRevision || len(intentRead.Values) != 1 ||
		intentRead.Values[0] == nil {
		if intentRead != nil {
			clearKeyValues(intentRead.Values)
		}
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup deletion intent is missing")
	}
	defer clearKeyValues(intentRead.Values)
	intent, err := decodeEnvironmentDeletionIntent(intentRead.Values[0].Value)
	if err != nil {
		return false, nil, false, err
	}
	if intent.EnvironmentID != environment.ID || intent.OperationID != lock.OperationID ||
		intent.TaskID != lock.TaskID || intent.TargetRevision != tombstone.TargetRevision ||
		!intent.CreatedAt.Equal(tombstone.CreatedAt) {
		return false, nil, false, errs.New(errs.KindStateConflict, "terminal backup deletion intent changed")
	}
	return true, &lock, true, nil
}

type backupTerminalReceiptPruneCompanion struct {
	key      string
	revision int64
}

func prepareBackupTerminalReceiptPruneCompanion(
	task TaskRecord,
	taskRevision int64,
	value *etcdstore.KeyValue,
) (backupTerminalReceiptPruneCompanion, error) {
	if task.Type != TaskBackup && task.Type != TaskBackupPrune {
		return backupTerminalReceiptPruneCompanion{}, errs.New(
			errs.KindInternal,
			"ordinary Task requested a Backup terminal receipt",
		)
	}
	if value == nil || value.ModRevision != taskRevision || value.Key != backupTerminalReceiptKey(task.ID) {
		return backupTerminalReceiptPruneCompanion{}, corruptTaskPruneIntent()
	}
	receipt, err := decodeBackupTerminalReceiptRecord(value.Value)
	if err != nil || receipt.PriorTaskRevision >= taskRevision ||
		validateBackupTerminalReceiptTaskBinding(task, receipt) != nil {
		return backupTerminalReceiptPruneCompanion{}, corruptTaskPruneIntent()
	}
	return backupTerminalReceiptPruneCompanion{key: value.Key, revision: value.ModRevision}, nil
}

func (companion backupTerminalReceiptPruneCompanion) appendStartCondition(
	conditions []etcdstore.Condition,
) []etcdstore.Condition {
	if companion.revision <= 0 {
		return conditions
	}
	return append(conditions, etcdstore.Condition{Key: companion.key, ModRevision: companion.revision})
}

func appendBackupTerminalReceiptPruneFinalization(
	intent taskPruneIntent,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) ([]etcdstore.Condition, []etcdstore.Mutation) {
	if intent.BackupTerminalReceiptRevision <= 0 {
		return conditions, mutations
	}
	key := backupTerminalReceiptKey(intent.TaskID)
	conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: intent.BackupTerminalReceiptRevision})
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
	return conditions, mutations
}
