package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintAttachRemovalPublication carries only the frozen authored removal
// recipe. Mutable task and Secret-pin publication state is prepared at the
// parent ledger revision immediately before the atomic child publication.
type BlueprintAttachRemovalPublication struct {
	intent    environmentchanges.AttachRemovalIntent
	hookInput *taskconfiguration.BackingHookEncryptedInputs
	hook      backingHookTaskPublication
}

func NewBlueprintAttachRemovalPublication(
	task TaskRecord,
	intent environmentchanges.AttachRemovalIntent,
	hookInput *taskconfiguration.BackingHookEncryptedInputs,
) (BlueprintAttachRemovalPublication, error) {
	if validateAuthoredAttachRemovalTaskOwner(task, intent) != nil {
		return BlueprintAttachRemovalPublication{}, errs.New(
			errs.KindValidationFailed, "Blueprint Attach removal publication is invalid",
		)
	}
	publication := BlueprintAttachRemovalPublication{
		intent: environmentchanges.CloneAttachRemovalIntent(intent),
	}
	if hookInput != nil {
		copy := *hookInput
		copy.Ciphertext = append([]byte(nil), hookInput.Ciphertext...)
		copy.SecretSources = append([]tasksecretpinrecord.Record(nil), hookInput.SecretSources...)
		publication.hookInput = &copy
	}
	return publication, nil
}

func (publication BlueprintAttachRemovalPublication) IsZero() bool {
	return publication.intent.TaskID == ""
}

func (publication *BlueprintAttachRemovalPublication) Clear() {
	if publication == nil {
		return
	}
	environmentchanges.ClearAttachRemovalIntent(&publication.intent)
	if publication.hookInput != nil {
		clear(publication.hookInput.Ciphertext)
	}
	publication.hook.clear()
	*publication = BlueprintAttachRemovalPublication{}
}

func (publication BlueprintAttachRemovalPublication) finish(
	ctx context.Context,
	repository *TaskRepository,
	cause error,
) error {
	if repository == nil {
		return cause
	}
	return publication.hook.finish(ctx, repository.store, cause)
}

func (publication *BlueprintAttachRemovalPublication) prepare(
	ctx context.Context,
	repository *TaskRepository,
	task TaskRecord,
	unit blueprintunits.Unit,
	revision int64,
) (TaskRecord, []keyvalue.Condition, []keyvalue.Mutation, error) {
	if publication == nil || repository == nil || publication.IsZero() ||
		unit.Target.Kind != ids.KindAttach || !unit.Removal || unit.Target.ID != publication.intent.AttachID ||
		validateAuthoredAttachRemovalTaskOwner(task, publication.intent) != nil {
		return TaskRecord{}, nil, nil, errs.New(
			errs.KindValidationFailed, "Blueprint Attach removal publication changed",
		)
	}
	hook, err := prepareBackingHookTaskPublication(ctx, repository.store, task, publication.hookInput)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	publication.hook = hook
	task = hook.task
	conditions, mutations, err := repository.prepareAuthoredAttachRemovalAuthority(
		ctx, task, publication.intent, revision,
	)
	if err != nil {
		return TaskRecord{}, nil, nil, err
	}
	classify := func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "Blueprint Attach removal authority changed concurrently")
	}
	conditions, mutations, _, err = hook.bind(conditions, mutations, classify)
	if err != nil {
		keyvalue.ClearMutationValues(mutations)
		return TaskRecord{}, nil, nil, err
	}
	return task, conditions, mutations, nil
}

func (repository *TaskRepository) prepareAuthoredAttachRemovalAuthority(
	ctx context.Context,
	task TaskRecord,
	intent environmentchanges.AttachRemovalIntent,
	revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if revision <= 0 || validateAuthoredAttachRemovalTaskOwner(task, intent) != nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "Blueprint Attach removal authority is invalid")
	}
	keys := []string{
		projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID),
		blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID),
		attachrecord.AttachKey(intent.AttachID),
		attachrecord.AttachFactsKey(intent.AttachID),
		environmentchanges.AttachRemovalIntentKey(task.ID),
		attachrender.AttachTaskRenderInputKey(task.PlanID),
		deletionrecord.TombstoneKey("attach", intent.AttachID),
		intent.BackingDesiredKey,
		servicerecord.ServiceRuntimeKey(intent.Source.BackingServiceID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil ||
		read.Values[4] != nil || read.Values[5] != nil || read.Values[6] != nil ||
		read.Values[7] == nil || read.Values[8] == nil {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Attach removal authority is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[0].Value)
	if err != nil || read.Values[0].ModRevision != intent.AppliedProjectionRevision ||
		projection.EnvironmentID != intent.AppliedProjection.EnvironmentID ||
		projection.RevisionID != intent.AppliedProjection.RevisionID ||
		projection.RenderGeneration != intent.AppliedProjection.RenderGeneration {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Attach applied projection changed")
	}
	headTaskID, err := idempotency.DecodeTaskReference(read.Values[1].Value)
	if err != nil || read.Values[1].ModRevision != intent.ExpectedHeadRevision || headTaskID != intent.ParentTaskID {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Attach removal parent changed")
	}
	current, err := attachrecord.DecodeAttachRecord(read.Values[2].Value)
	if err != nil || read.Values[2].ModRevision != intent.SourceRevision ||
		!attachrecord.AttachImmutableEqual(current, intent.Source) || current.Status != intent.Source.Status ||
		current.Operation != intent.Source.Operation || current.TaskID != intent.Source.TaskID {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Attach removal source changed")
	}
	if (read.Values[3] == nil) != (intent.FactsRevision == 0) ||
		read.Values[3] != nil && read.Values[3].ModRevision != intent.FactsRevision ||
		read.Values[7].ModRevision != intent.BackingDesiredRevision ||
		read.Values[8].ModRevision != intent.BackingRuntimeRevision {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Attach removal inputs changed")
	}
	source := keyvalue.Versioned[attachrecord.Record]{
		Record: current, Revision: read.Values[2].ModRevision, ReadRevision: revision,
	}
	referenceConditions, _, values, err := attachrecord.PrepareAttachRemoval(
		ctx, repository.store, source, revision,
	)
	for _, value := range values {
		clear(value)
	}
	if err != nil {
		return nil, nil, err
	}
	topologyConditions, err := repository.authoredAttachRemovalTopologyConditions(ctx, intent, revision)
	if err != nil {
		return nil, nil, err
	}
	hierarchyConditions := []keyvalue.Condition{
		{Key: hierarchyrecord.TenantKey(task.Owner.TenantID), ModRevision: intent.TenantRevision},
		{Key: hierarchyrecord.ProjectKey(task.Owner.ProjectID), ModRevision: intent.ProjectRevision},
		{Key: hierarchyrecord.EnvironmentKey(intent.EnvironmentID), ModRevision: intent.EnvironmentRevision},
		{Key: hierarchyrecord.ProjectKey(intent.Source.BackingProjectID), ModRevision: intent.BackingProjectRevision},
		{
			Key:         hierarchyrecord.EnvironmentKey(intent.Source.BackingEnvironmentID),
			ModRevision: intent.BackingEnvironmentRevision,
		},
	}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, intent.EnvironmentID, revision)
	if err != nil {
		return nil, nil, err
	}
	epochRevision, found := fence.EpochRevision()
	if !found || epochRevision != intent.RenderInput.EnvironmentEpochRevision {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Attach runtime epoch changed")
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return nil, nil, err
	}
	intentValue, err := environmentchanges.EncodeAttachRemovalIntent(intent)
	if err != nil {
		clear(epoch.Value)
		return nil, nil, err
	}
	renderValue, err := attachrender.EncodeAttachTaskRenderInput(intent.RenderInput)
	if err != nil {
		clear(epoch.Value)
		clear(intentValue)
		return nil, nil, err
	}
	detachingValue, err := attachrecord.EncodeAttachRecord(intent.Detaching)
	if err != nil {
		clear(epoch.Value)
		clear(intentValue)
		clear(renderValue)
		return nil, nil, err
	}
	planKey, planValue, _, err := prepareAttachTaskPlanReference(task)
	if err != nil {
		clear(epoch.Value)
		clear(intentValue)
		clear(renderValue)
		clear(detachingValue)
		return nil, nil, err
	}
	conditions := []keyvalue.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision},
		{Key: keys[3], ModRevision: keyvalue.RevisionOf(read.Values[3])},
		{Key: keys[4]}, {Key: keys[5]}, {Key: keys[6]},
		{Key: keys[7], ModRevision: read.Values[7].ModRevision},
		{Key: keys[8], ModRevision: read.Values[8].ModRevision},
		{Key: planKey},
	}
	conditions = append(conditions, referenceConditions...)
	conditions = append(conditions, topologyConditions...)
	conditions = append(conditions, hierarchyConditions...)
	conditions = append(conditions, fence.TransactionConditions()...)
	conditions = append(conditions, attachRuntimeSourceConditions(intent.RenderInput)...)
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: attachrecord.AttachKey(intent.AttachID), Value: detachingValue},
		{Type: keyvalue.MutationPut, Key: keys[4], Value: intentValue},
		{Type: keyvalue.MutationPut, Key: keys[5], Value: renderValue},
		{Type: keyvalue.MutationPut, Key: planKey, Value: planValue},
		epoch,
	}
	return conditions, mutations, nil
}

func (repository *TaskRepository) authoredAttachRemovalTopologyConditions(
	ctx context.Context,
	intent environmentchanges.AttachRemovalIntent,
	revision int64,
) ([]keyvalue.Condition, error) {
	keys := make([]string, len(intent.Topology))
	for index, source := range intent.Topology {
		keys[index] = attachrecord.AttachKey(source.Record.ID)
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, errs.New(errs.KindInternal, "Blueprint Attach topology read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, len(keys))
	for index, source := range intent.Topology {
		value := read.Values[index]
		if value == nil || value.ModRevision != source.Revision {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Attach topology changed")
		}
		record, err := attachrecord.DecodeAttachRecord(value.Value)
		if err != nil || !attachrecord.AttachImmutableEqual(record, source.Record) ||
			record.Status != source.Record.Status || record.Operation != source.Record.Operation ||
			record.TaskID != source.Record.TaskID {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Attach topology record changed")
		}
		conditions[index] = keyvalue.Condition{Key: keys[index], ModRevision: value.ModRevision}
	}
	return conditions, nil
}

func validateAuthoredAttachRemovalTaskOwner(
	task TaskRecord,
	intent environmentchanges.AttachRemovalIntent,
) error {
	if environmentchanges.ValidateAttachRemovalIntent(intent) != nil || task.ID != intent.TaskID ||
		task.Owner.EnvironmentID != intent.EnvironmentID || task.Type != taskjournal.TaskDetach ||
		task.Target != intent.AttachID || task.Params[taskjournal.TaskBlueprintParentParam] != intent.ParentTaskID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != intent.ParentTaskID ||
		task.Params[taskjournal.TaskMutationEnvironmentParam] != intent.EnvironmentID {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach removal Task ownership changed")
	}
	return nil
}
