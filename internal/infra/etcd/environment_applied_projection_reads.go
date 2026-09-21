package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
)

func (repository *HierarchyRepository) ListEnvironmentAppliedComposeProjections(
	ctx context.Context,
) ([]etcdstore.Versioned[EnvironmentComposeProjection], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	result := make([]etcdstore.Versioned[EnvironmentComposeProjection], 0)
	start := ""
	var revision int64
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: environmentComposeProjectionPrefix, StartExclusive: start,
			Limit: 128, Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if page == nil {
			return nil, errs.New(errs.KindInternal, "Environment applied projection scan is empty")
		}
		if revision == 0 {
			revision = page.ReadRevision
		}
		if page.More && len(page.Values) == 0 {
			return nil, errs.New(errs.KindInternal, "Environment applied projection scan did not advance")
		}
		for _, value := range page.Values {
			if !strings.HasPrefix(value.Key, environmentComposeProjectionPrefix) {
				clearRangeKeyValues(page.Values)
				return nil, corruptEnvironmentComposeProjection()
			}
			environmentID := strings.TrimPrefix(value.Key, environmentComposeProjectionPrefix)
			if ids.Validate(ids.KindEnvironment, environmentID) != nil {
				clearRangeKeyValues(page.Values)
				return nil, corruptEnvironmentComposeProjection()
			}
			projection, err := decodeEnvironmentComposeProjection(value.Value)
			if err != nil || projection.EnvironmentID != environmentID {
				clearRangeKeyValues(page.Values)
				return nil, corruptEnvironmentComposeProjection()
			}
			result = append(result, etcdstore.Versioned[EnvironmentComposeProjection]{
				Record: projection, Revision: value.ModRevision, ReadRevision: revision,
			})
			start = value.Key
		}
		clearRangeKeyValues(page.Values)
		if !page.More {
			return result, nil
		}
	}
}

// GetEnvironmentAppliedComposeProjection returns the mutable projection last
// acknowledged by the runtime. It is distinct from the immutable desired
// Blueprint head returned by GetEnvironmentComposeProjection.
func (repository *HierarchyRepository) GetEnvironmentAppliedComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[EnvironmentComposeProjection], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[EnvironmentComposeProjection]{}, false, err
	}
	result, err := repository.store.Get(ctx, environmentComposeProjectionKey(environmentID))
	if err != nil {
		return etcdstore.Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if result == nil || result.Entry == nil {
		readRevision := int64(0)
		if result != nil {
			readRevision = result.ReadRevision
		}
		return etcdstore.Versioned[EnvironmentComposeProjection]{ReadRevision: readRevision}, false, nil
	}
	projection, err := decodeEnvironmentComposeProjection(result.Entry.Value)
	if err != nil || projection.EnvironmentID != environmentID {
		return etcdstore.Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	return etcdstore.Versioned[EnvironmentComposeProjection]{
		Record: projection, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
