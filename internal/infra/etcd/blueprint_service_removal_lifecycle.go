package etcd

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareAuthoredServiceRemovalAcknowledgement(
	ctx context.Context, task TaskRecord, intent environmentchanges.ServiceRemovalIntent,
	intentRevision int64, terminalStatus taskjournal.TaskStatus, terminalAt time.Time, revision int64,
) (removalTaskChange, error) {
	keys := authoredServiceRemovalStateKeys(intent)
	state, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return removalTaskChange{}, err
	}
	if state == nil || state.ReadRevision != revision || len(state.Values) != len(keys) ||
		state.Values[2] == nil || state.Values[3] == nil ||
		state.Values[4] == nil || state.Values[5] == nil || state.Values[6] != nil ||
		!keyvalue.ConditionMatchesRead(
			keyvalue.Condition{Key: keys[0], ModRevision: intent.RuntimeRevision}, state.Values[0],
		) ||
		keyvalue.RevisionOf(state.Values[1]) != intent.AcknowledgedRuntimeRevision ||
		state.Values[4].ModRevision != intent.CurrentProjectionRevision ||
		string(state.Values[5].Value) != task.ID {
		return removalTaskChange{}, errs.New(errs.KindStateConflict, "authored Service removal terminal state changed")
	}
	defer keyvalue.ClearValues(state.Values)
	definitionGuards, err := repository.declaredServiceRemovalAppliedAbsence(ctx, intent, revision)
	if err != nil {
		return removalTaskChange{}, err
	}
	if terminalStatus == taskjournal.TaskStatusCompleted {
		headTaskID, err := idempotencyrecord.DecodeTaskReference(state.Values[3].Value)
		if err != nil || state.Values[3].ModRevision != intent.ExpectedHeadRevision ||
			headTaskID != intent.Authored.ParentTaskID {
			return removalTaskChange{}, errs.New(errs.KindStateConflict, "authored Service removal head changed")
		}
	}
	applied, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(state.Values[4].Value)
	if err != nil || !environmentchanges.SameServiceRemovalProjection(applied, intent.CurrentProjection) {
		return removalTaskChange{}, errs.New(errs.KindStateConflict, "authored Service removal projection changed")
	}
	if intent.AcknowledgedRuntime != nil {
		wantRuntime, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", *intent.AcknowledgedRuntime)
		if err != nil || state.Values[1] == nil || !bytes.Equal(wantRuntime, state.Values[1].Value) {
			clear(wantRuntime)
			return removalTaskChange{}, errs.New(errs.KindStateConflict, "authored Service removal runtime changed")
		}
		clear(wantRuntime)
	} else if state.Values[1] != nil {
		return removalTaskChange{}, errs.New(errs.KindStateConflict, "authored Service acknowledged runtime appeared")
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(state.Values[2].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetService ||
		tombstone.TargetID != intent.ServiceID || tombstone.TargetRevision != intent.ServiceRevision ||
		tombstone.TaskID != task.ID || tombstone.Phase != deletionrecord.DeletionPhaseHostEffects {
		return removalTaskChange{}, errs.New(errs.KindStateConflict, "authored Service removal tombstone changed")
	}
	terminal, err := environmentchanges.TerminalServiceRemovalIntent(intent, terminalStatus, terminalAt)
	if err != nil {
		return removalTaskChange{}, err
	}
	terminalValue, err := environmentchanges.EncodeServiceRemovalIntent(terminal)
	if err != nil {
		return removalTaskChange{}, err
	}
	change := removalTaskChange{
		applies: true,
		conditions: []keyvalue.Condition{
			{Key: environmentchanges.ServiceRemovalIntentKey(task.ID), ModRevision: intentRevision},
			{Key: keys[0], ModRevision: keyvalue.RevisionOf(state.Values[0])},
			{Key: keys[1], ModRevision: keyvalue.RevisionOf(state.Values[1])},
			{Key: keys[2], ModRevision: state.Values[2].ModRevision},
			{Key: keys[3], ModRevision: state.Values[3].ModRevision},
			{Key: keys[4], ModRevision: state.Values[4].ModRevision},
			{Key: keys[5], ModRevision: state.Values[5].ModRevision},
			{Key: keys[6]},
		},
		mutations: []keyvalue.Mutation{
			{
				Type:  keyvalue.MutationPut,
				Key:   environmentchanges.ServiceRemovalIntentKey(task.ID),
				Value: terminalValue,
			},
			{Type: keyvalue.MutationDelete, Key: keys[2]},
			{Type: keyvalue.MutationDelete, Key: keys[5]},
		},
		values: [][]byte{terminalValue},
	}
	change.conditions = append(change.conditions, definitionGuards...)
	if terminalStatus != taskjournal.TaskStatusCompleted {
		return change, nil
	}
	scriptConditions, err := scriptsourceevidence.PrepareServiceScriptAbsence(
		ctx, repository.store, intent.ServiceID, revision,
	)
	if err != nil {
		clearRemovalTaskChange(change)
		return removalTaskChange{}, err
	}
	dnsConditions, err := dnsrecords.NewReader(repository.store).
		RequireDNSUnreferenced(ctx, intent.ServiceID, "", revision)
	if err != nil {
		clearRemovalTaskChange(change)
		return removalTaskChange{}, err
	}
	change.conditions = append(change.conditions, dnsConditions...)
	addresses, err := networkreservations.PrepareProxyAddressRelease(ctx, repository.store, intent.ServiceID, revision)
	if err != nil {
		clearRemovalTaskChange(change)
		return removalTaskChange{}, err
	}
	if intent.Authored.ProjectionSource == environmentchanges.ServiceRemovalAppliedProjection {
		projectionValue, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(intent.CandidateProjection)
		if err != nil {
			clearRemovalTaskChange(change)
			return removalTaskChange{}, err
		}
		change.values = append(change.values, projectionValue)
		change.mutations = append(change.mutations, keyvalue.Mutation{
			Type: keyvalue.MutationPut, Key: keys[4], Value: projectionValue,
		})
	}
	change.conditions = append(change.conditions, scriptConditions...)
	change.conditions = append(change.conditions, addresses.Conditions()...)
	change.mutations = append(change.mutations, addresses.Mutations()...)
	for _, mutation := range addresses.Mutations() {
		change.values = append(change.values, mutation.Value)
	}
	change.mutations = append(change.mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[0]},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[1]},
	)
	return change, nil
}

func (repository *TaskRepository) validateAuthoredServiceRemovalAcknowledgementReplay(
	ctx context.Context, task TaskRecord, intent environmentchanges.ServiceRemovalIntent,
	terminalStatus taskjournal.TaskStatus, revision int64,
) error {
	keys := authoredServiceRemovalStateKeys(intent)
	state, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != len(keys) || state.Values[2] != nil || state.Values[5] != nil {
		return errs.New(errs.KindStateConflict, "authored Service removal terminal fence is inconsistent")
	}
	defer keyvalue.ClearValues(state.Values)
	if _, err := repository.declaredServiceRemovalAppliedAbsence(ctx, intent, revision); err != nil {
		return err
	}
	wantProjection := intent.CurrentProjection
	if terminalStatus == taskjournal.TaskStatusCompleted {
		if intent.Authored.ProjectionSource == environmentchanges.ServiceRemovalAppliedProjection {
			wantProjection = intent.CandidateProjection
		}
		if state.Values[0] != nil || state.Values[1] != nil {
			return errs.New(errs.KindStateConflict, "completed authored Service removal retained runtime")
		}
	} else if !keyvalue.ConditionMatchesRead(
		keyvalue.Condition{Key: keys[0], ModRevision: intent.RuntimeRevision}, state.Values[0],
	) || keyvalue.RevisionOf(state.Values[1]) != intent.AcknowledgedRuntimeRevision {
		return errs.New(errs.KindStateConflict, "failed authored Service removal lost runtime")
	}
	if state.Values[4] == nil {
		return errs.New(errs.KindStateConflict, "authored Service removal projection is missing")
	}
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(state.Values[4].Value)
	if err != nil || !environmentchanges.SameServiceRemovalProjection(projection, wantProjection) {
		return errs.New(errs.KindStateConflict, "authored Service removal terminal projection changed")
	}
	return nil
}

func authoredServiceRemovalStateKeys(intent environmentchanges.ServiceRemovalIntent) []string {
	return []string{
		servicerecord.ServiceRuntimeKey(intent.ServiceID),
		serviceruntimerecord.Key(intent.ServiceID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetService), intent.ServiceID),
		blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID),
		environmentchanges.ServiceRemovalProjectionKey(intent),
		environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID),
		servicerecord.ServiceLifecycleActiveKey(intent.ServiceID),
	}
}
