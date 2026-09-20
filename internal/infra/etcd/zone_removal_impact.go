package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backingZoneImpactPageSize = int64(128)

// ListAttachesByBackingNetworkAtRevision returns the complete stable-id ordered
// dependency set selected by a backing Project and its owned network.
func (repository *AttachRepository) ListAttachesByBackingNetworkAtRevision(
	ctx context.Context,
	backingProjectID string,
	networkID string,
	revision int64,
) ([]Versioned[AttachRecord], error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if repository == nil || repository.store == nil || validateID(ids.KindProject, backingProjectID) != nil ||
		validateID(ids.KindNetwork, networkID) != nil || revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "backing Zone impact scope is invalid")
	}
	prefix := attachBackingProjectPrefix(backingProjectID)
	start := ""
	indexes := make([]etcdstore.KeyValue, 0)
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: backingZoneImpactPageSize, Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision != revision {
			return nil, errs.New(errs.KindInternal, "backing Zone Attach index snapshot is inconsistent")
		}
		indexes = append(indexes, page.Values...)
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return nil, errs.New(errs.KindInternal, "backing Zone Attach index pagination made no progress")
		}
		start = page.Values[len(page.Values)-1].Key
	}
	if len(indexes) == 0 {
		return []Versioned[AttachRecord]{}, nil
	}
	keys := make([]string, len(indexes))
	idsByIndex := make([]string, len(indexes))
	for index, value := range indexes {
		attachID := strings.TrimPrefix(value.Key, prefix)
		if attachID == value.Key || strings.Contains(attachID, "/") || validateID(ids.KindAttach, attachID) != nil ||
			string(value.Value) != attachID {
			return nil, errs.New(errs.KindInternal, "backing Zone Attach index is corrupt")
		}
		keys[index] = attachKey(attachID)
		idsByIndex[index] = attachID
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if stored == nil || stored.ReadRevision != revision || len(stored.Values) != len(keys) {
		return nil, errs.New(errs.KindInternal, "backing Zone Attach snapshot is incomplete")
	}
	result := make([]Versioned[AttachRecord], 0, len(keys))
	for index, value := range stored.Values {
		if value == nil {
			return nil, errs.New(errs.KindInternal, "backing Zone Attach record is missing")
		}
		record, err := decodeAttachRecord(value.Value)
		if err != nil || record.ID != idsByIndex[index] || record.BackingProjectID != backingProjectID {
			return nil, corruptAttachRecord()
		}
		if record.BackingNetworkID != networkID {
			continue
		}
		result = append(result, Versioned[AttachRecord]{
			Record: record, Revision: value.ModRevision, ReadRevision: revision,
		})
	}
	slices.SortFunc(result, func(left, right Versioned[AttachRecord]) int {
		return strings.Compare(left.Record.ID, right.Record.ID)
	})
	return result, nil
}

func attachBackingProjectPrefix(projectID string) string {
	return "/v1/indexes/attaches/by-backing-project/project/" + projectID + "/"
}
