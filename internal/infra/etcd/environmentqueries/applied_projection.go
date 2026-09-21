package environmentqueries

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
)

func (repository *ProjectionReader) ListEnvironmentAppliedComposeProjections(
	ctx context.Context,
) ([]etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	result := make([]etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], 0)
	start := ""
	var revision int64
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: projectionrecord.EnvironmentComposeProjectionPrefix, StartExclusive: start,
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
			if !strings.HasPrefix(value.Key, projectionrecord.EnvironmentComposeProjectionPrefix) {
				recordquery.ClearRangeKeyValues(page.Values)
				return nil, projectionrecord.CorruptEnvironmentComposeProjection()
			}
			environmentID := strings.TrimPrefix(value.Key, projectionrecord.EnvironmentComposeProjectionPrefix)
			if ids.Validate(ids.KindEnvironment, environmentID) != nil {
				recordquery.ClearRangeKeyValues(page.Values)
				return nil, projectionrecord.CorruptEnvironmentComposeProjection()
			}
			projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(value.Value)
			if err != nil || projection.EnvironmentID != environmentID {
				recordquery.ClearRangeKeyValues(page.Values)
				return nil, projectionrecord.CorruptEnvironmentComposeProjection()
			}
			result = append(result, etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
				Record: projection, Revision: value.ModRevision, ReadRevision: revision,
			})
			start = value.Key
		}
		recordquery.ClearRangeKeyValues(page.Values)
		if !page.More {
			return result, nil
		}
	}
}

// GetEnvironmentAppliedComposeProjection returns the mutable projection last
// acknowledged by the runtime. It is distinct from the immutable desired
// Blueprint head returned by GetEnvironmentComposeProjection.
func (repository *ProjectionReader) GetEnvironmentAppliedComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	result, err := repository.store.Get(ctx, projectionrecord.EnvironmentComposeProjectionStorageKey(environmentID))
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if result == nil || result.Entry == nil {
		readRevision := int64(0)
		if result != nil {
			readRevision = result.ReadRevision
		}
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
			ReadRevision: readRevision,
		}, false, nil
	}
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(result.Entry.Value)
	if err != nil || projection.EnvironmentID != environmentID {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, projectionrecord.CorruptEnvironmentComposeProjection()
	}
	return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record: projection, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
