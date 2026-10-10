package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalreferences"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BlueprintServiceRemovalPublication struct {
	intent environmentchanges.ServiceRemovalIntent
}

func NewBlueprintServiceRemovalPublication(
	task TaskRecord, intent environmentchanges.ServiceRemovalIntent,
) (BlueprintServiceRemovalPublication, error) {
	if intent.Authored == nil || validateServiceRemovalTaskOwner(task, intent) != nil {
		return BlueprintServiceRemovalPublication{}, errs.New(
			errs.KindValidationFailed, "Blueprint Service removal publication is invalid",
		)
	}
	return BlueprintServiceRemovalPublication{intent: environmentchanges.CloneServiceRemovalIntent(intent)}, nil
}

func (publication BlueprintServiceRemovalPublication) IsZero() bool {
	return publication.intent.TaskID == ""
}

func (publication *BlueprintServiceRemovalPublication) Clear() {
	if publication == nil {
		return
	}
	if publication.intent.AcknowledgedRuntime != nil {
		clear(publication.intent.AcknowledgedRuntime.Runtime.CurrentArtifact)
		clear(publication.intent.AcknowledgedRuntime.Runtime.RetainedPriorArtifact)
	}
	clear(publication.intent.CurrentProjection.ComposeArtifact)
	clear(publication.intent.CandidateProjection.ComposeArtifact)
	*publication = BlueprintServiceRemovalPublication{}
}

func (publication BlueprintServiceRemovalPublication) prepare(
	ctx context.Context, repository *TaskRepository, task TaskRecord,
	unit blueprintunits.Unit, revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	intent := publication.intent
	if repository == nil || publication.IsZero() || intent.Authored == nil ||
		unit.Target.Kind != ids.KindService || !unit.Removal || unit.Target.ID != intent.ServiceID ||
		validateServiceRemovalTaskOwner(task, intent) != nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "Blueprint Service removal publication changed")
	}
	keys := []string{
		projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID),
		servicerecord.ServiceRuntimeKey(intent.ServiceID), serviceruntimerecord.Key(intent.ServiceID),
		blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetService), intent.ServiceID),
		servicerecord.ServiceLifecycleActiveKey(intent.ServiceID),
		environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[3] == nil ||
		read.Values[4] != nil || read.Values[5] != nil || read.Values[6] != nil {
		return nil, nil, errs.New(errs.KindResourceInUse, "Blueprint Service removal authority is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	appliedRecord, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[0].Value)
	if err != nil || read.Values[0].ModRevision != intent.CurrentProjectionRevision ||
		!environmentchanges.SameServiceRemovalProjection(appliedRecord, intent.CurrentProjection) {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Service applied projection changed")
	}
	applied := keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record: appliedRecord, Revision: read.Values[0].ModRevision, ReadRevision: revision,
	}
	current, err := servicerecord.ReadJoined(ctx, repository.store, servicerecord.DesiredSelection{
		Services: applied.Record.DesiredServices, Revision: applied.Revision, ReadRevision: revision,
	}, intent.ServiceID, keys[0])
	if err != nil || current.Revision != intent.ServiceRevision ||
		servicerecord.ServiceRuntimeRevision(current) != intent.RuntimeRevision ||
		current.Record.EnvironmentID != intent.EnvironmentID || current.Record.Desired.Name != intent.ServiceName {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Service removal predecessor changed")
	}
	if !keyvalue.ConditionMatchesRead(servicerecord.ServiceRuntimeCondition(current), read.Values[1]) ||
		keyvalue.RevisionOf(read.Values[2]) != intent.AcknowledgedRuntimeRevision ||
		read.Values[3].ModRevision != intent.ExpectedHeadRevision {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Service runtime changed")
	}
	headTaskID, err := idempotencyrecord.DecodeTaskReference(read.Values[3].Value)
	if err != nil || headTaskID != intent.Authored.ParentTaskID {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Service removal head changed")
	}
	if intent.AcknowledgedRuntime != nil {
		wantRuntime, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", *intent.AcknowledgedRuntime)
		if err != nil || read.Values[2] == nil || !bytes.Equal(wantRuntime, read.Values[2].Value) {
			clear(wantRuntime)
			return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Service acknowledged runtime changed")
		}
		clear(wantRuntime)
	} else if read.Values[2] != nil {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Service acknowledged runtime appeared")
	}
	guards, err := serviceremovalreferences.Validate(ctx, repository.store, current, applied)
	if err != nil {
		return nil, nil, err
	}
	dnsConditions, err := dnsrecords.NewReader(repository.store).RequireDNSUnreferenced(
		ctx, intent.ServiceID, "", revision,
	)
	if err != nil {
		return nil, nil, err
	}
	scriptConditions, err := scriptsourceevidence.PrepareServiceScriptAbsence(
		ctx, repository.store, intent.ServiceID, revision,
	)
	if err != nil {
		return nil, nil, err
	}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, intent.EnvironmentID, revision)
	if err != nil {
		return nil, nil, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return nil, nil, err
	}
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetService, TargetID: intent.ServiceID,
		TargetRevision: intent.ServiceRevision, TaskID: task.ID, Phase: deletionrecord.DeletionPhaseHostEffects,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	})
	if err != nil {
		clear(epoch.Value)
		return nil, nil, err
	}
	intentValue, err := environmentchanges.EncodeServiceRemovalIntent(intent)
	if err != nil {
		clear(epoch.Value)
		clear(tombstoneValue)
		return nil, nil, err
	}
	conditions := []keyvalue.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: keyvalue.RevisionOf(read.Values[1])},
		{Key: keys[2], ModRevision: keyvalue.RevisionOf(read.Values[2])},
		{Key: keys[3], ModRevision: read.Values[3].ModRevision},
		{Key: keys[4]}, {Key: keys[5]}, {Key: keys[6]},
		{Key: environmentchanges.ServiceRemovalIntentKey(task.ID)},
	}
	conditions = append(conditions, guards.Conditions()...)
	conditions = append(conditions, dnsConditions...)
	conditions = append(conditions, scriptConditions...)
	conditions = append(conditions, fence.TransactionConditions()...)
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: keys[4], Value: tombstoneValue},
		{Type: keyvalue.MutationPut, Key: environmentchanges.ServiceRemovalIntentKey(task.ID), Value: intentValue},
		{Type: keyvalue.MutationPut, Key: keys[6], Value: []byte(task.ID)}, epoch,
	}
	return conditions, mutations, nil
}
