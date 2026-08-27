package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const releaseGroupCollectionEpochPrefix = "/v1/runtime/release-group-collection-epochs/"

type releaseGroupCollectionReader interface {
	GetMany(context.Context, GetManyRequest) (*GetManyResult, error)
}

type releaseGroupCollectionEpochRecord struct {
	EnvironmentID string `json:"environment_id"`
}

func releaseGroupCollectionEpochKey(environmentID string) string {
	return releaseGroupCollectionEpochPrefix + environmentID
}

func encodeReleaseGroupCollectionEpoch(environmentID string) ([]byte, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return nil, errs.New(errs.KindInternal, "release group collection epoch scope is invalid")
	}
	return encodeEnvelope(
		"release-group-collection-epoch",
		releaseGroupCollectionEpochRecord{EnvironmentID: environmentID},
	)
}

func validateReleaseGroupCollectionEpoch(value []byte, environmentID string) error {
	record, err := decodeEnvelope[releaseGroupCollectionEpochRecord](
		value,
		"release-group-collection-epoch",
	)
	if err != nil || record.EnvironmentID != environmentID {
		return corruptRecord()
	}
	return nil
}

func loadReleaseGroupCollectionEpoch(
	ctx context.Context,
	store releaseGroupCollectionReader,
	environmentID string,
	revision int64,
) (Condition, Mutation, error) {
	result, err := store.GetMany(ctx, GetManyRequest{
		Keys:     []string{releaseGroupCollectionEpochKey(environmentID)},
		Revision: revision,
	})
	if err != nil {
		return Condition{}, Mutation{}, err
	}
	if result == nil || len(result.Values) != 1 ||
		(revision > 0 && result.ReadRevision != revision) {
		return Condition{}, Mutation{}, corruptRecord()
	}
	condition := Condition{Key: releaseGroupCollectionEpochKey(environmentID)}
	if result.Values[0] != nil {
		if err := validateReleaseGroupCollectionEpoch(result.Values[0].Value, environmentID); err != nil {
			return Condition{}, Mutation{}, err
		}
		condition.ModRevision = result.Values[0].ModRevision
	}
	value, err := encodeReleaseGroupCollectionEpoch(environmentID)
	if err != nil {
		return Condition{}, Mutation{}, err
	}
	return condition, Mutation{
		Type:  MutationPut,
		Key:   releaseGroupCollectionEpochKey(environmentID),
		Value: value,
	}, nil
}
