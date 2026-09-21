package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareServiceRemovalTaskRetry(
	ctx context.Context,
	source TaskRecord,
	revision int64,
) (routeTaskChange, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{serviceRemovalIntentKey(source.ID)}, Revision: revision,
	})
	if err != nil {
		return routeTaskChange{}, err
	}
	if result == nil || len(result.Values) != 1 {
		return routeTaskChange{}, errs.New(errs.KindInternal, "Service removal retry read is incomplete")
	}
	if result.Values[0] == nil {
		return routeTaskChange{}, nil
	}
	return routeTaskChange{}, errs.New(
		errs.KindStateConflict,
		"failed Service removal must be reissued with DELETE so a fresh desired candidate is sealed",
	)
}

func (repository *TaskRepository) prepareServiceRemovalTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
	revision int64,
) (routeTaskChange, error) {
	intentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{serviceRemovalIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return routeTaskChange{}, err
	}
	if intentRead == nil || len(intentRead.Values) != 1 {
		return routeTaskChange{}, errs.New(errs.KindInternal, "Service removal intent read is incomplete")
	}
	intentValue := intentRead.Values[0]
	if intentValue == nil {
		return routeTaskChange{}, nil
	}
	intent, err := decodeServiceRemovalIntent(intentValue.Value)
	if err != nil {
		return routeTaskChange{}, err
	}
	if err := validateServiceRemovalTaskOwner(task, intent); err != nil {
		return routeTaskChange{}, err
	}
	if intent.Status != taskjournal.TaskStatusPending {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Service removal intent is not pending")
	}
	keys := []string{
		servicerecord.ServiceRuntimeKey(intent.ServiceID),
		deletionTombstoneKey(string(deletionrecord.DeletionTargetService), intent.ServiceID),
		blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID),
		projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID),
		componentTaskActiveEnvironmentKey(intent.EnvironmentID),
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if state == nil || len(state.Values) != len(keys) || state.Values[1] == nil ||
		state.Values[2] == nil || state.Values[3] == nil || state.Values[4] == nil ||
		!conditionMatchesRead(etcdstore.Condition{Key: keys[0], ModRevision: intent.RuntimeRevision}, state.Values[0]) ||
		state.Values[2].ModRevision != intent.ExpectedHeadRevision ||
		state.Values[3].ModRevision != intent.CurrentProjectionRevision ||
		string(state.Values[4].Value) != task.ID {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Service removal terminal state changed")
	}
	foundDesired := false
	for _, desired := range intent.CurrentProjection.DesiredServices {
		if desired.EnvironmentID == intent.EnvironmentID && desired.Desired.ID == intent.ServiceID &&
			desired.Desired.Name == intent.ServiceName {
			foundDesired = true
			break
		}
	}
	if !foundDesired {
		return routeTaskChange{}, corruptServiceRemovalIntent()
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(state.Values[1].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetService || tombstone.TargetID != intent.ServiceID ||
		tombstone.TargetRevision != intent.ServiceRevision || tombstone.TaskID != task.ID ||
		tombstone.Phase != deletionrecord.DeletionPhaseHostEffects {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Service removal tombstone changed")
	}
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(state.Values[3].Value)
	if err != nil || !sameRouteRemovalProjection(projection, intent.CurrentProjection) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Service removal projection changed")
	}
	terminalIntent, err := terminalServiceRemovalIntent(intent, terminalStatus, terminalAt)
	if err != nil {
		return routeTaskChange{}, err
	}
	terminalValue, err := encodeServiceRemovalIntent(terminalIntent)
	if err != nil {
		return routeTaskChange{}, err
	}
	change := routeTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: serviceRemovalIntentKey(task.ID), ModRevision: intentValue.ModRevision},
			{Key: keys[0], ModRevision: intent.RuntimeRevision},
			{Key: keys[1], ModRevision: state.Values[1].ModRevision},
			{Key: keys[2], ModRevision: state.Values[2].ModRevision},
			{Key: keys[3], ModRevision: state.Values[3].ModRevision},
			{Key: keys[4], ModRevision: state.Values[4].ModRevision},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: serviceRemovalIntentKey(task.ID), Value: terminalValue},
			{Type: etcdstore.MutationDelete, Key: keys[1]},
			{Type: etcdstore.MutationDelete, Key: keys[4]},
		},
		values: [][]byte{terminalValue},
	}
	if terminalStatus != taskjournal.TaskStatusCompleted {
		return change, nil
	}
	scriptConditions, err := prepareServiceScriptAbsence(ctx, repository.store, intent.ServiceID, revision)
	if err != nil {
		clearRouteTaskChange(change)
		return routeTaskChange{}, err
	}
	change.conditions = append(change.conditions, scriptConditions...)
	hierarchy := &HierarchyRepository{store: repository.store}
	publication, err := hierarchy.prepareEnvironmentDirectPublication(
		ctx,
		intent.Claim,
		EnvironmentDesiredRevisionIdentity{EnvironmentID: intent.EnvironmentID, RevisionID: intent.Claim.RevisionID},
		intent.CandidateProjection,
		idempotencyrecord.IdempotencyMarker{Locator: intent.Claim.Locator, Intent: intent.Claim.Intent},
		intent.ExpectedHeadRevision,
	)
	if err != nil {
		clearRouteTaskChange(change)
		return routeTaskChange{}, err
	}
	headReference, err := idempotencyrecord.EncodeTaskReference(intent.Claim.RevisionID)
	if err != nil {
		clear(publication.publishedDescriptor)
		clearRouteTaskChange(change)
		return routeTaskChange{}, err
	}
	candidateValue, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(intent.CandidateProjection)
	if err != nil {
		clear(publication.publishedDescriptor)
		clear(headReference)
		clearRouteTaskChange(change)
		return routeTaskChange{}, err
	}
	change.values = append(change.values, publication.publishedDescriptor, headReference, candidateValue)
	change.conditions = append(
		change.conditions,
		etcdstore.Condition{
			Key:         environmentBlueprintRootKey(intent.EnvironmentID, intent.Claim.RevisionID),
			ModRevision: publication.rootRevision,
		},
		etcdstore.Condition{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		etcdstore.Condition{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
	)
	change.mutations = append(change.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: publication.locatorKey},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[2], Value: headReference},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[3], Value: candidateValue},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[0]},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: serviceruntimerecord.Key(intent.ServiceID)},
	)
	return change, nil
}

func (repository *TaskRepository) validateServiceRemovalTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{serviceRemovalIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if result == nil || len(result.Values) != 1 {
		return errs.New(errs.KindInternal, "Service removal replay read is incomplete")
	}
	if result.Values[0] == nil {
		return nil
	}
	intent, err := decodeServiceRemovalIntent(result.Values[0].Value)
	if err != nil {
		return err
	}
	if err := validateServiceRemovalTaskOwner(task, intent); err != nil {
		return err
	}
	if intent.Status != terminalStatus || intent.TerminalAt == nil || task.FinishedAt == nil ||
		!intent.TerminalAt.Equal(*task.FinishedAt) {
		return errs.New(errs.KindStateConflict, "Service removal intent does not match terminal Task")
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		servicerecord.ServiceRuntimeKey(intent.ServiceID), deletionTombstoneKey(string(deletionrecord.DeletionTargetService), intent.ServiceID),
		blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID), projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID),
		componentTaskActiveEnvironmentKey(intent.EnvironmentID),
	}, Revision: revision})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != 5 || state.Values[1] != nil || state.Values[2] == nil ||
		state.Values[3] == nil || state.Values[4] != nil {
		return errs.New(errs.KindStateConflict, "Service removal terminal fence is inconsistent")
	}
	wantRevision := intent.ExpectedHeadRevision
	wantProjection := intent.CurrentProjection
	if terminalStatus == taskjournal.TaskStatusCompleted {
		if state.Values[0] != nil {
			return errs.New(errs.KindStateConflict, "completed Service removal retained its target")
		}
		wantRevision = state.Values[2].ModRevision
		wantProjection = intent.CandidateProjection
	} else if !conditionMatchesRead(etcdstore.Condition{Key: servicerecord.ServiceRuntimeKey(intent.ServiceID), ModRevision: intent.RuntimeRevision}, state.Values[0]) {
		return errs.New(errs.KindStateConflict, "failed Service removal lost its target")
	}
	projection, decodeErr := projectionrecord.DecodeEnvironmentComposeProjectionStorage(state.Values[3].Value)
	if state.Values[2].ModRevision != wantRevision || decodeErr != nil ||
		!sameRouteRemovalProjection(projection, wantProjection) {
		return errs.New(errs.KindStateConflict, "Service removal terminal desired state is inconsistent")
	}
	return nil
}
