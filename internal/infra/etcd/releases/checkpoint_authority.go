package releases

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"slices"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ReleaseCheckpointAuthority struct {
	store etcdstore.Store
}

func NewReleaseCheckpointAuthority(store etcdstore.Store) (*ReleaseCheckpointAuthority, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "release checkpoint store is not configured")
	}
	return &ReleaseCheckpointAuthority{store: store}, nil
}

type ReleaseCheckpointAdvance struct {
	PublicationID string
	OperationID   string
	EnvironmentID string
	ReleaseID     string
	NextState     domain.State
	Evidence      *domain.EffectEvidence
	Now           time.Time
}

type ReleaseCheckpointResult struct {
	Checkpoint domain.Checkpoint
	Projection domain.ServiceProjection
	Revision   int64
	Duplicate  bool
}

type releaseCheckpointTransaction struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	checkpoint domain.Checkpoint
	projection domain.ServiceProjection
	head       ReleaseOperationHead
	duplicate  bool
}

func (authority *ReleaseCheckpointAuthority) Advance(
	ctx context.Context,
	input ReleaseCheckpointAdvance,
) (ReleaseCheckpointResult, error) {
	prepared, err := authority.prepareAdvance(ctx, input)
	if err != nil {
		return ReleaseCheckpointResult{}, err
	}
	defer prepared.clear()
	if prepared.duplicate {
		return ReleaseCheckpointResult{
			Checkpoint: prepared.checkpoint,
			Projection: prepared.projection,
			Duplicate:  true,
		}, nil
	}
	result, err := authority.store.Transact(ctx, prepared.conditions, prepared.mutations)
	if err != nil {
		return ReleaseCheckpointResult{}, err
	}
	if !result.Succeeded {
		return ReleaseCheckpointResult{}, errs.New(errs.KindStateConflict, "release checkpoint evidence changed")
	}
	return ReleaseCheckpointResult{
		Checkpoint: prepared.checkpoint,
		Projection: prepared.projection,
		Revision:   result.Revision,
	}, nil
}

func (authority *ReleaseCheckpointAuthority) prepareAdvance(
	ctx context.Context,
	input ReleaseCheckpointAdvance,
) (releaseCheckpointTransaction, error) {
	if ctx == nil || authority == nil || authority.store == nil || ValidatePublicationID(input.PublicationID) != nil ||
		input.Now.IsZero() || input.Now.Location() != time.UTC {
		return releaseCheckpointTransaction{}, errs.New(
			errs.KindValidationFailed,
			"release checkpoint advance is invalid",
		)
	}
	if err := ctx.Err(); err != nil {
		return releaseCheckpointTransaction{}, err
	}
	keys := []string{
		ReleasePublicationKey(input.PublicationID), ReleaseOperationKey(input.OperationID),
		ReleaseCheckpointStagingKey(input.PublicationID, input.ReleaseID), ReleaseFenceSetKey(input.EnvironmentID),
		hierarchyrecord.EnvironmentMutationEpochKey(input.EnvironmentID),
	}
	loaded, err := authority.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return releaseCheckpointTransaction{}, err
	}
	if loaded == nil || len(loaded.Values) != len(keys) {
		return releaseCheckpointTransaction{}, CorruptReleaseRecord()
	}
	for index := range loaded.Values {
		if loaded.Values[index] == nil {
			return releaseCheckpointTransaction{}, CorruptReleaseRecord()
		}
	}
	marker, err := DecodeReleaseRecord[ReleasePublicationMarker](loaded.Values[0].Value, "release-publication")
	if err != nil || marker.PublicationID != input.PublicationID || marker.OperationID != input.OperationID {
		return releaseCheckpointTransaction{}, CorruptReleaseRecord()
	}
	head, err := DecodeReleaseRecord[ReleaseOperationHead](loaded.Values[1].Value, "release-operation")
	if err != nil || head.OperationID != input.OperationID || head.EnvironmentID != input.EnvironmentID ||
		head.State.Terminal() || head.State == domain.StateRecoveryRequired {
		return releaseCheckpointTransaction{}, errs.New(
			errs.KindStateConflict,
			"release operation does not accept checkpoints",
		)
	}
	checkpoint, err := DecodeReleaseRecord[domain.Checkpoint](loaded.Values[2].Value, "release-checkpoint")
	if err != nil || checkpoint.ReleaseID != input.ReleaseID || domain.ValidateCheckpoint(checkpoint) != nil {
		return releaseCheckpointTransaction{}, CorruptReleaseRecord()
	}
	fence, err := DecodeReleaseRecord[ReleaseFenceSet](loaded.Values[3].Value, "release-fence-set")
	if err != nil || fence.OperationID != input.OperationID || fence.EnvironmentID != input.EnvironmentID {
		return releaseCheckpointTransaction{}, errs.New(
			errs.KindStateConflict,
			"release checkpoint fence ownership changed",
		)
	}
	member, found := releaseFenceMember(fence, input.ReleaseID)
	if !found {
		return releaseCheckpointTransaction{}, errs.New(
			errs.KindStateConflict,
			"release checkpoint candidate is not fenced",
		)
	}
	if input.Evidence != nil {
		if err := domain.ValidateEvidence(*input.Evidence, input.ReleaseID); err != nil {
			return releaseCheckpointTransaction{}, err
		}
		for _, existing := range checkpoint.Evidence {
			if existing.AcknowledgementID != input.Evidence.AcknowledgementID {
				continue
			}
			if existing != *input.Evidence || checkpoint.State != input.NextState {
				return releaseCheckpointTransaction{}, errs.New(
					errs.KindStateConflict,
					"release acknowledgement evidence changed",
				)
			}
			projection, _, err := authority.loadProjectionAt(ctx, member.ServiceID, loaded.ReadRevision)
			return releaseCheckpointTransaction{
				checkpoint: domain.CloneCheckpoint(checkpoint), projection: projection,
				head: CloneReleaseOperationHead(head), duplicate: true,
			}, err
		}
	}
	if !domain.CanTransition(checkpoint.State, input.NextState) {
		return releaseCheckpointTransaction{}, errs.New(
			errs.KindStateConflict,
			"release checkpoint transition is not monotonic",
		)
	}
	next := domain.CloneCheckpoint(checkpoint)
	next.State = input.NextState
	next.UpdatedAt = input.Now
	if input.Evidence != nil {
		next.Evidence = append(next.Evidence, *input.Evidence)
	}
	if err := domain.ValidateCheckpoint(next); err != nil {
		return releaseCheckpointTransaction{}, err
	}
	projection, projectionRevision, err := authority.loadProjectionAt(ctx, member.ServiceID, loaded.ReadRevision)
	if err != nil {
		return releaseCheckpointTransaction{}, err
	}
	if projection.EnvironmentID == "" {
		projection = domain.ServiceProjection{
			EnvironmentID:     input.EnvironmentID,
			ServiceID:         member.ServiceID,
			ActiveOperationID: input.OperationID,
			Revision:          1,
		}
	} else if projection.EnvironmentID != input.EnvironmentID || projection.ServiceID != member.ServiceID ||
		(projection.ActiveOperationID != "" && projection.ActiveOperationID != input.OperationID) {
		return releaseCheckpointTransaction{}, errs.New(errs.KindStateConflict, "service release projection ownership changed")
	} else {
		projection.ActiveOperationID = input.OperationID
	}
	if input.NextState == domain.StateServing {
		projection.ServingReleaseID = input.ReleaseID
		projection.ServingSlot = memberSlot(input.Evidence)
		projection.Revision++
	}
	checkpointValue, err := EncodeReleaseRecord("release-checkpoint", next)
	if err != nil {
		return releaseCheckpointTransaction{}, err
	}
	projectionValue, err := EncodeReleaseRecord("service-release-projection", projection)
	if err != nil {
		clear(checkpointValue)
		return releaseCheckpointTransaction{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: keys[0], ModRevision: loaded.Values[0].ModRevision},
		{Key: keys[1], ModRevision: loaded.Values[1].ModRevision},
		{Key: keys[2], ModRevision: loaded.Values[2].ModRevision},
		{Key: keys[3], ModRevision: loaded.Values[3].ModRevision},
		{Key: keys[4], ModRevision: loaded.Values[4].ModRevision},
		{Key: ReleaseProjectionKey(member.ServiceID), ModRevision: projectionRevision},
	}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: keys[2], Value: checkpointValue}}
	if input.NextState == domain.StateServing {
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   ReleaseProjectionKey(member.ServiceID),
				Value: projectionValue,
			},
		)
	} else {
		clear(projectionValue)
	}
	return releaseCheckpointTransaction{
		conditions: conditions, mutations: mutations, checkpoint: next,
		projection: projection, head: CloneReleaseOperationHead(head),
	}, nil
}

func (prepared *releaseCheckpointTransaction) clear() {
	if prepared == nil {
		return
	}
	etcdstore.ZeroMutationBytes(prepared.mutations)
	prepared.conditions = nil
	prepared.mutations = nil
}

func (authority *ReleaseCheckpointAuthority) loadProjectionAt(
	ctx context.Context,
	serviceID string,
	revision int64,
) (domain.ServiceProjection, int64, error) {
	result, err := authority.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{ReleaseProjectionKey(serviceID)}, Revision: revision},
	)
	if err != nil {
		return domain.ServiceProjection{}, 0, err
	}
	if result == nil || len(result.Values) != 1 {
		return domain.ServiceProjection{}, 0, CorruptReleaseRecord()
	}
	if result.Values[0] == nil {
		return domain.ServiceProjection{}, 0, nil
	}
	value, err := DecodeReleaseRecord[domain.ServiceProjection](result.Values[0].Value, "service-release-projection")
	if err != nil {
		return domain.ServiceProjection{}, 0, err
	}
	return value, result.Values[0].ModRevision, nil
}

func releaseFenceMember(fence ReleaseFenceSet, releaseID string) (ReleaseFenceMember, bool) {
	for _, member := range fence.Members {
		if member.CandidateReleaseID == releaseID {
			return member, true
		}
	}
	return ReleaseFenceMember{}, false
}

func memberSlot(evidence *domain.EffectEvidence) domain.Slot {
	if evidence == nil {
		return ""
	}
	return evidence.ObservedSlot
}

func CloneReleaseOperationHead(value ReleaseOperationHead) ReleaseOperationHead {
	value.Attempts = slices.Clone(value.Attempts)
	value.Members = slices.Clone(value.Members)
	if value.Progress != nil {
		progress := *value.Progress
		progress.Results = slices.Clone(value.Progress.Results)
		value.Progress = &progress
	}
	return value
}
