package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareServiceRemovalTaskRetry(
	ctx context.Context,
	source TaskRecord,
	revision int64,
) (routeTaskChange, error) {
	result, err := repository.store.GetMany(ctx, GetManyRequest{
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
	terminalStatus TaskStatus,
	terminalAt time.Time,
	revision int64,
) (routeTaskChange, error) {
	intentRead, err := repository.store.GetMany(ctx, GetManyRequest{
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
	if intent.Status != TaskStatusPending {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Service removal intent is not pending")
	}
	keys := []string{
		serviceKey(intent.ServiceID),
		deletionTombstoneKey(string(DeletionTargetService), intent.ServiceID),
		serviceNameKey(intent.EnvironmentID, intent.ServiceName),
		serviceOwnerKey(intent.EnvironmentID, intent.ServiceID),
		environmentBlueprintHeadKey(intent.EnvironmentID),
		environmentComposeProjectionKey(intent.EnvironmentID),
		componentTaskActiveEnvironmentKey(intent.EnvironmentID),
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if state == nil || len(state.Values) != len(keys) || state.Values[0] == nil || state.Values[1] == nil ||
		state.Values[2] == nil || state.Values[3] == nil || state.Values[4] == nil || state.Values[5] == nil ||
		state.Values[6] == nil || state.Values[0].ModRevision != intent.ServiceRevision ||
		state.Values[4].ModRevision != intent.ExpectedHeadRevision ||
		state.Values[5].ModRevision != intent.CurrentProjectionRevision ||
		string(state.Values[2].Value) != intent.ServiceID || string(state.Values[3].Value) != intent.ServiceID ||
		string(state.Values[6].Value) != task.ID {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Service removal terminal state changed")
	}
	record, err := decodeServiceRecord(state.Values[0].Value)
	if err != nil || record.Desired.ID != intent.ServiceID || record.Desired.Name != intent.ServiceName ||
		record.EnvironmentID != intent.EnvironmentID {
		return routeTaskChange{}, corruptRecord()
	}
	tombstone, err := decodeDeletionTombstone(state.Values[1].Value)
	if err != nil || tombstone.TargetKind != DeletionTargetService || tombstone.TargetID != intent.ServiceID ||
		tombstone.TargetRevision != intent.ServiceRevision || tombstone.TaskID != task.ID ||
		tombstone.Phase != DeletionPhaseHostEffects {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Service removal tombstone changed")
	}
	projection, err := decodeEnvironmentComposeProjection(state.Values[5].Value)
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
		conditions: []Condition{
			{Key: serviceRemovalIntentKey(task.ID), ModRevision: intentValue.ModRevision},
			{Key: keys[0], ModRevision: state.Values[0].ModRevision},
			{Key: keys[1], ModRevision: state.Values[1].ModRevision},
			{Key: keys[2], ModRevision: state.Values[2].ModRevision},
			{Key: keys[3], ModRevision: state.Values[3].ModRevision},
			{Key: keys[4], ModRevision: state.Values[4].ModRevision},
			{Key: keys[5], ModRevision: state.Values[5].ModRevision},
			{Key: keys[6], ModRevision: state.Values[6].ModRevision},
		},
		mutations: []Mutation{
			{Type: MutationPut, Key: serviceRemovalIntentKey(task.ID), Value: terminalValue},
			{Type: MutationDelete, Key: keys[1]},
			{Type: MutationDelete, Key: keys[6]},
		},
		values: [][]byte{terminalValue},
	}
	if terminalStatus != TaskStatusCompleted {
		return change, nil
	}
	hierarchy := &HierarchyRepository{store: repository.store}
	publication, err := hierarchy.prepareEnvironmentDirectPublication(
		ctx,
		intent.Claim,
		EnvironmentDesiredRevisionIdentity{EnvironmentID: intent.EnvironmentID, RevisionID: intent.Claim.RevisionID},
		intent.CandidateProjection,
		IdempotencyMarker{Locator: intent.Claim.Locator, Intent: intent.Claim.Intent},
		intent.ExpectedHeadRevision,
	)
	if err != nil {
		clearRouteTaskChange(change)
		return routeTaskChange{}, err
	}
	headReference, err := encodeTaskReference(intent.Claim.RevisionID)
	if err != nil {
		clear(publication.publishedDescriptor)
		clearRouteTaskChange(change)
		return routeTaskChange{}, err
	}
	candidateValue, err := encodeEnvironmentComposeProjection(intent.CandidateProjection)
	if err != nil {
		clear(publication.publishedDescriptor)
		clear(headReference)
		clearRouteTaskChange(change)
		return routeTaskChange{}, err
	}
	change.values = append(change.values, publication.publishedDescriptor, headReference, candidateValue)
	change.conditions = append(change.conditions,
		Condition{Key: environmentBlueprintRootKey(intent.EnvironmentID, intent.Claim.RevisionID), ModRevision: publication.rootRevision},
		Condition{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		Condition{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
	)
	change.mutations = append(change.mutations,
		Mutation{Type: MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		Mutation{Type: MutationDelete, Key: publication.locatorKey},
		Mutation{Type: MutationPut, Key: keys[4], Value: headReference},
		Mutation{Type: MutationPut, Key: keys[5], Value: candidateValue},
		Mutation{Type: MutationDelete, Key: keys[2]},
		Mutation{Type: MutationDelete, Key: keys[3]},
		Mutation{Type: MutationDelete, Key: keys[0]},
	)
	return change, nil
}

func (repository *TaskRepository) validateServiceRemovalTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) error {
	result, err := repository.store.GetMany(ctx, GetManyRequest{
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
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		serviceKey(intent.ServiceID), deletionTombstoneKey(string(DeletionTargetService), intent.ServiceID),
		environmentBlueprintHeadKey(intent.EnvironmentID), environmentComposeProjectionKey(intent.EnvironmentID),
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
	if terminalStatus == TaskStatusCompleted {
		if state.Values[0] != nil {
			return errs.New(errs.KindStateConflict, "completed Service removal retained its target")
		}
		wantRevision = state.Values[2].ModRevision
		wantProjection = intent.CandidateProjection
	} else if state.Values[0] == nil || state.Values[0].ModRevision != intent.ServiceRevision {
		return errs.New(errs.KindStateConflict, "failed Service removal lost its target")
	}
	projection, decodeErr := decodeEnvironmentComposeProjection(state.Values[3].Value)
	if state.Values[2].ModRevision != wantRevision || decodeErr != nil ||
		!sameRouteRemovalProjection(projection, wantProjection) {
		return errs.New(errs.KindStateConflict, "Service removal terminal desired state is inconsistent")
	}
	return nil
}
