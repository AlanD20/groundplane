package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const TaskEntryRuntimeEpochParam = "entry_runtime_epoch_revision"

// EntryTaskRuntime pins operational intent separately from desired input. An
// explicit empty set means materialization only; absent capture is not authority
// to reconstruct or publish an Entry update.
type EntryTaskRuntime struct {
	RunningServiceIDs []string             `json:"running_service_ids"`
	Updates           []EntryRuntimeUpdate `json:"updates"`
}

// EntryRuntimeUpdate is the bounded durable recipe for one selected Service.
// Runtime bytes remain in the prior receipt and immutable Entry projection.
type EntryRuntimeUpdate struct {
	ServiceID               string `json:"service_id"`
	PreviousRevision        int64  `json:"previous_revision"`
	CurrentArtifactID       string `json:"current_artifact_id"`
	RetainedPriorArtifactID string `json:"retained_prior_artifact_id,omitempty"`
}

func validateTaskEntryRuntime(task TaskRecord) error {
	if task.EntryRuntime == nil {
		// Historical Tasks remain readable without inventing capture authority.
		return nil
	}
	if task.Type != taskjournal.TaskUpdate || task.Executor != taskjournal.TaskExecutorAgent ||
		task.Params[TaskResourceKindParam] != TaskResourceEntry || task.EntryRuntime.RunningServiceIDs == nil ||
		task.EntryRuntime.Updates == nil {
		return errs.New(errs.KindValidationFailed, "Entry runtime capture shape is invalid")
	}
	running := make(map[string]struct{}, len(task.EntryRuntime.RunningServiceIDs))
	for index, id := range task.EntryRuntime.RunningServiceIDs {
		if ids.Validate(ids.KindService, id) != nil ||
			index > 0 && task.EntryRuntime.RunningServiceIDs[index-1] >= id {
			return errs.New(errs.KindValidationFailed, "Entry running Service ids must be valid, sorted and unique")
		}
		running[id] = struct{}{}
	}
	for index, update := range task.EntryRuntime.Updates {
		_, selected := running[update.ServiceID]
		if !selected || update.PreviousRevision <= 0 || ids.Validate(ids.KindConfig, update.CurrentArtifactID) != nil ||
			(update.RetainedPriorArtifactID != "" &&
				(ids.Validate(ids.KindConfig, update.RetainedPriorArtifactID) != nil ||
					update.RetainedPriorArtifactID == update.CurrentArtifactID)) ||
			index > 0 && task.EntryRuntime.Updates[index-1].ServiceID >= update.ServiceID {
			return errs.New(errs.KindValidationFailed, "Entry runtime update is invalid")
		}
	}
	return nil
}

func cloneEntryTaskRuntime(runtime *EntryTaskRuntime) *EntryTaskRuntime {
	if runtime == nil {
		return nil
	}
	cloned := &EntryTaskRuntime{RunningServiceIDs: slices.Clone(runtime.RunningServiceIDs),
		Updates: slices.Clone(runtime.Updates)}
	return cloned
}

func EntryRuntimeEpochRevision(task TaskRecord) (int64, error) {
	if task.EntryRuntime == nil {
		return 0, errs.New(errs.KindValidationFailed, "Entry runtime capture is absent")
	}
	if err := validateTaskEntryRuntime(task); err != nil {
		return 0, err
	}
	value := task.Params[TaskEntryRuntimeEpochParam]
	epoch, err := strconv.ParseInt(value, 10, 64)
	if err != nil || epoch <= 0 || strconv.FormatInt(epoch, 10) != value ||
		task.Type != taskjournal.TaskUpdate || task.Params[TaskResourceKindParam] != TaskResourceEntry {
		return 0, errs.New(errs.KindValidationFailed, "Entry runtime capture epoch is invalid")
	}
	return epoch, nil
}

func validateEntryRuntimePublication(task TaskRecord, fence environmentMutationFenceEvidence) error {
	entryMutation := task.Type == taskjournal.TaskUpdate && task.Params[TaskResourceKindParam] == TaskResourceEntry
	if !entryMutation && task.Params[TaskEntryRuntimeEpochParam] == "" && task.EntryRuntime == nil {
		return nil
	}
	epoch, err := EntryRuntimeEpochRevision(task)
	if err != nil {
		return err
	}
	for _, condition := range fence.conditions {
		if condition.kind != environmentMutationFenceEpoch {
			continue
		}
		if condition.modRevision != epoch {
			return errs.New(errs.KindStateConflict, "Entry serving runtime changed before publication")
		}
		return nil
	}
	return errs.New(errs.KindInternal, "Entry runtime publication has no Environment epoch fence")
}

func entryRuntimeSourceConditions(task TaskRecord) []etcdstore.Condition {
	if task.EntryRuntime == nil {
		return nil
	}
	conditions := make([]etcdstore.Condition, len(task.EntryRuntime.Updates))
	for index, update := range task.EntryRuntime.Updates {
		conditions[index] = etcdstore.Condition{
			Key:         serviceruntimerecord.Key(update.ServiceID),
			ModRevision: update.PreviousRevision,
		}
	}
	return conditions
}

func bindEntryRuntimePublication(
	task TaskRecord, conditions []etcdstore.Condition, classify func(int64, []*etcdstore.KeyValue) error,
) ([]etcdstore.Condition, func(int64, []*etcdstore.KeyValue) error) {
	sourceConditions := entryRuntimeSourceConditions(task)
	base := len(conditions)
	conditions = append(conditions, sourceConditions...)
	return conditions, func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != base+len(sourceConditions) {
			return errs.New(errs.KindInternal, "Entry runtime publication evidence is incomplete")
		}
		if err := classify(revision, values[:base]); err != nil {
			return err
		}
		for index, condition := range sourceConditions {
			value := values[base+index]
			if value == nil || value.ModRevision != condition.ModRevision {
				return errs.New(errs.KindStateConflict, "Entry acknowledged runtime changed before publication")
			}
		}
		return nil
	}
}

func (repository *TaskRepository) entryRuntimeClaimConditions(
	ctx context.Context, task TaskRecord, revision int64,
) ([]etcdstore.Condition, error) {
	conditions := entryRuntimeSourceConditions(task)
	if len(conditions) == 0 {
		return nil, nil
	}
	keys := make([]string, len(conditions))
	for index := range conditions {
		keys[index] = conditions[index].Key
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, errs.New(errs.KindInternal, "Entry runtime claim source read is incomplete")
	}
	defer clearKeyValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.ModRevision != conditions[index].ModRevision {
			return nil, errs.New(errs.KindStateConflict, "Entry acknowledged runtime changed before execution")
		}
	}
	return conditions, nil
}

func (repository *TaskRepository) prepareEntryRuntimeAcknowledgement(
	ctx context.Context, terminal TaskRecord, assignment taskassignments.TaskAssignmentRecord, revision int64,
) (taskMaterializationProjectionChange, error) {
	if terminal.Type != taskjournal.TaskUpdate || terminal.Params[TaskResourceKindParam] != TaskResourceEntry ||
		terminal.Status != taskjournal.TaskStatusCompleted || terminal.EntryRuntime == nil || len(terminal.EntryRuntime.Updates) == 0 {
		return taskMaterializationProjectionChange{}, nil
	}
	result := terminal.Result
	if result == nil || result.Kind != taskjournal.TaskResultCompose || result.ExitCode != 0 ||
		result.Diagnostic != taskjournal.TaskResultDiagnosticNone || result.ReconciliationRequired ||
		result.ExecutionEpoch == 0 || result.ExecutionEpoch != assignment.ExecutionEpoch || terminal.FinishedAt == nil ||
		terminal.TerminalAssignment == nil || terminal.TerminalAssignment.AssignmentID != assignment.AssignmentID ||
		terminal.TerminalAssignment.AgentID != assignment.AgentID {
		return taskMaterializationProjectionChange{}, errs.New(
			errs.KindStateConflict, "Entry runtime requires an exact successful assignment",
		)
	}
	stepID, err := entryRuntimeComposeStep(terminal)
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	revisionID := terminal.Params[EnvironmentDesiredRevisionParam]
	hierarchy := &HierarchyRepository{store: repository.store}
	candidate, found, err := hierarchy.GetEnvironmentComposeProjectionRevision(ctx, terminal.Target, revisionID)
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	if !found || candidate.Record.RenderGeneration != uint64(terminal.RenderGeneration) {
		return taskMaterializationProjectionChange{}, errs.New(
			errs.KindStateConflict,
			"Entry runtime candidate is unavailable",
		)
	}
	artifact := new(agentpb.ComposeArtifact)
	if proto.Unmarshal(candidate.Record.ComposeArtifact, artifact) != nil {
		return taskMaterializationProjectionChange{}, errs.New(errs.KindInternal, "Entry runtime candidate is corrupt")
	}
	conditions := entryRuntimeSourceConditions(terminal)
	keys := make([]string, len(conditions))
	for index := range conditions {
		keys[index] = conditions[index].Key
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return taskMaterializationProjectionChange{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return taskMaterializationProjectionChange{}, errs.New(
			errs.KindInternal,
			"Entry runtime terminal source read is incomplete",
		)
	}
	defer clearKeyValues(read.Values)
	proof, err := json.Marshal(struct {
		Updates []EntryRuntimeUpdate        `json:"updates"`
		Result  *taskjournal.TaskResultData `json:"result"`
	}{terminal.EntryRuntime.Updates, taskjournal.TaskResultToData(result)})
	if err != nil {
		return taskMaterializationProjectionChange{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(proof)
	clear(proof)
	change := taskMaterializationProjectionChange{applies: true, conditions: conditions}
	for index, update := range terminal.EntryRuntime.Updates {
		value := read.Values[index]
		if value == nil || value.ModRevision != update.PreviousRevision {
			clearTaskMaterializationProjectionChange(change)
			return taskMaterializationProjectionChange{}, errs.New(
				errs.KindStateConflict, "Entry acknowledged runtime changed before acknowledgement",
			)
		}
		previous, decodeErr := decodeAcknowledgedServiceRuntime(value.Value, terminal.Target, update.ServiceID)
		if decodeErr != nil {
			clearTaskMaterializationProjectionChange(change)
			return taskMaterializationProjectionChange{}, decodeErr
		}
		prepared, prepareErr := executionplan.PrepareEntryRuntime(
			artifact, previous.Runtime, update.CurrentArtifactID, update.RetainedPriorArtifactID,
		)
		if prepareErr != nil {
			clearTaskMaterializationProjectionChange(change)
			return taskMaterializationProjectionChange{}, prepareErr
		}
		record := serviceruntimerecord.Record{EnvironmentID: terminal.Target, Runtime: prepared,
			Source: serviceruntimerecord.Acknowledgement{
				TaskID: terminal.ID, PlanID: terminal.PlanID, PlanHash: terminal.PlanHash, StepID: stepID,
				AgentID: assignment.AgentID, AssignmentID: assignment.AssignmentID,
				ExecutionEpoch: assignment.ExecutionEpoch, RenderGeneration: uint64(terminal.RenderGeneration),
				EffectDigest: hex.EncodeToString(digest[:]), AcknowledgedAt: *terminal.FinishedAt,
			}}
		record.Configuration, err = taskRuntimeConfiguration(terminal)
		if err != nil {
			clearTaskMaterializationProjectionChange(change)
			return taskMaterializationProjectionChange{}, err
		}
		if validateErr := serviceruntimerecord.Validate(record); validateErr != nil {
			clearTaskMaterializationProjectionChange(change)
			return taskMaterializationProjectionChange{}, validateErr
		}
		encoded, encodeErr := releases.EncodeReleaseRecord("service-acknowledged-runtime", record)
		if encodeErr != nil {
			clearTaskMaterializationProjectionChange(change)
			return taskMaterializationProjectionChange{}, encodeErr
		}
		change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[index], Value: encoded})
	}
	return change, nil
}

func entryRuntimeComposeStep(task TaskRecord) (string, error) {
	materializations := make(map[string]struct{}, len(task.Materializations))
	for _, materialization := range task.Materializations {
		materializations[materialization.StepID] = struct{}{}
	}
	stepID := ""
	for _, step := range task.Steps {
		if _, materializes := materializations[step.ID]; materializes {
			continue
		}
		if stepID != "" || step.Kind != taskjournal.TaskStepOperation {
			return "", errs.New(errs.KindStateConflict, "Entry runtime Compose step is ambiguous")
		}
		stepID = step.ID
	}
	if stepID == "" {
		return "", errs.New(errs.KindStateConflict, "Entry runtime Compose step is absent")
	}
	return stepID, nil
}
