package serviceremovalreferences

import (
	"bytes"
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const releaseGroupPageSize = int64(64)

type referenceStore interface {
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

// Guards carries exact reference compares into Service removal publication.
type Guards struct {
	conditions []etcdstore.Condition
}

// Conditions returns detached publication compares.
func (guards Guards) Conditions() []etcdstore.Condition {
	return append([]etcdstore.Condition(nil), guards.conditions...)
}

// Prepare rejects fixed-revision Release Group and explicit Entry references,
// then captures the Release Group collection epoch that closes publication races.
func Prepare(
	ctx context.Context,
	store referenceStore,
	current servicerecord.ServiceRecord,
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
) (Guards, error) {
	checkCondition, err := backingchecks.RequireServiceIdle(ctx, store, current.Desired.ID, projection.ReadRevision)
	if err != nil {
		return Guards{}, err
	}
	edgeCondition, err := edgechecks.RequireServiceIdle(ctx, store, current.Desired.ID, projection.ReadRevision)
	if err != nil {
		return Guards{}, err
	}
	for _, entry := range projection.Record.Entries {
		if !entry.Entry.ExposesAll() && slices.Contains(entry.Entry.Exposure, current.Desired.Name) {
			return Guards{}, errs.New(errs.KindResourceInUse, "Service is referenced by an Entry exposure")
		}
	}
	if err := rejectReleaseGroupReferences(ctx, store, current.EnvironmentID, current.Desired.ID, projection.ReadRevision); err != nil {
		return Guards{}, err
	}
	condition, rewrite, err := groupstore.LoadReleaseGroupCollectionEpoch(
		ctx, store, current.EnvironmentID, projection.ReadRevision,
	)
	clear(rewrite.Value)
	if err != nil {
		return Guards{}, err
	}
	return Guards{conditions: []etcdstore.Condition{condition, checkCondition, edgeCondition}}, nil
}

func rejectReleaseGroupReferences(
	ctx context.Context,
	store referenceStore,
	environmentID, serviceID string,
	revision int64,
) error {
	prefix := groupstore.ReleaseGroupOwnerPrefix + environmentID + "/"
	start := ""
	for {
		page, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: releaseGroupPageSize, Revision: revision,
		})
		if err != nil {
			return err
		}
		if page == nil || page.ReadRevision != revision {
			return recordcodec.CorruptRecord()
		}
		keys := make([]string, len(page.Values))
		for index, owner := range page.Values {
			groupID := strings.TrimPrefix(owner.Key, prefix)
			if groupID == owner.Key || ids.Validate(ids.KindReleaseGroup, groupID) != nil ||
				!bytes.Equal(owner.Value, []byte(groupID)) {
				etcdstore.ClearRangeValues(page.Values)
				return recordcodec.CorruptRecord()
			}
			keys[index] = groupstore.ReleaseGroupRecordKey(groupID)
			start = owner.Key
		}
		more := page.More
		etcdstore.ClearRangeValues(page.Values)
		if err := rejectReleaseGroupPage(ctx, store, environmentID, serviceID, revision, keys); err != nil {
			return err
		}
		if !more {
			return nil
		}
		if start == "" {
			return recordcodec.CorruptRecord()
		}
	}
}

func rejectReleaseGroupPage(
	ctx context.Context,
	store referenceStore,
	environmentID, serviceID string,
	revision int64,
	keys []string,
) error {
	if len(keys) == 0 {
		return nil
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) {
		return recordcodec.CorruptRecord()
	}
	defer etcdstore.ClearValues(result.Values)
	for _, stored := range result.Values {
		if stored == nil {
			return recordcodec.CorruptRecord()
		}
		group, err := groupstore.DecodeReleaseGroupStored(stored.Value)
		if err != nil || group.EnvironmentID != environmentID {
			return recordcodec.CorruptRecord()
		}
		if slices.Contains(group.ServiceIDs, serviceID) {
			return errs.New(errs.KindResourceInUse, "Service is referenced by a Release Group")
		}
	}
	return nil
}
