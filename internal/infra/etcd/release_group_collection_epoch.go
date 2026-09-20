package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const releaseGroupCollectionEpochPrefix = "/v1/runtime/release-group-collection-epochs/"

type releaseGroupCollectionReader interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
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
	return recordcodec.Encode(
		"release-group-collection-epoch",
		releaseGroupCollectionEpochRecord{EnvironmentID: environmentID},
	)
}

func validateReleaseGroupCollectionEpoch(value []byte, environmentID string) error {
	record, err := recordcodec.Decode[releaseGroupCollectionEpochRecord](
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
) (etcdstore.Condition, etcdstore.Mutation, error) {
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{releaseGroupCollectionEpochKey(environmentID)},
		Revision: revision,
	})
	if err != nil {
		return etcdstore.Condition{}, etcdstore.Mutation{}, err
	}
	if result == nil || len(result.Values) != 1 ||
		(revision > 0 && result.ReadRevision != revision) {
		return etcdstore.Condition{}, etcdstore.Mutation{}, corruptRecord()
	}
	condition := etcdstore.Condition{Key: releaseGroupCollectionEpochKey(environmentID)}
	if result.Values[0] != nil {
		if err := validateReleaseGroupCollectionEpoch(result.Values[0].Value, environmentID); err != nil {
			return etcdstore.Condition{}, etcdstore.Mutation{}, err
		}
		condition.ModRevision = result.Values[0].ModRevision
	}
	value, err := encodeReleaseGroupCollectionEpoch(environmentID)
	if err != nil {
		return etcdstore.Condition{}, etcdstore.Mutation{}, err
	}
	return condition, etcdstore.Mutation{
		Type:  etcdstore.MutationPut,
		Key:   releaseGroupCollectionEpochKey(environmentID),
		Value: value,
	}, nil
}
