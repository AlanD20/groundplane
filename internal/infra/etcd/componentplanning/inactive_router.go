package componentplanning

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// InactiveRouterConditions includes retained runtimes whose Component is no
// longer authored. Desired omission alone is not proof that a router stopped.
// The caller also holds the Environment claim and compares its desired head
// and mutation epoch when committing the Controller-only decision.
func InactiveRouterConditions(ctx context.Context, store interface {
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
}, environmentID string, kinds []core.ComponentKind, revision int64) ([]keyvalue.Condition, error) {
	if ctx == nil || store == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil || revision <= 0 || len(kinds) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Router absence requires its Environment snapshot and registered provider kinds")
	}
	var conditions []keyvalue.Condition
	cursor := ""
	for {
		page, err := store.Range(ctx, keyvalue.RangeRequest{Prefix: componentRuntimePrefix, StartExclusive: cursor, Limit: 128, Revision: revision})
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision != revision || page.More && len(page.Values) == 0 {
			return nil, errs.New(errs.KindStateConflict, "Router runtime snapshot is unavailable")
		}
		for index := range page.Values {
			value := &page.Values[index]
			if !strings.HasPrefix(value.Key, componentRuntimePrefix) || value.Key <= cursor || value.ModRevision <= 0 || value.ModRevision > revision {
				clearRuntimePage(page)
				return nil, recordcodec.CorruptRecord()
			}
			record, err := recordcodec.Decode[Runtime](value.Value, "blueprint-component-runtime")
			if err != nil || ValidateRuntime(record) != nil || RuntimeKey(record.Component.Desired.ID) != value.Key {
				clearRuntimePage(page)
				return nil, recordcodec.CorruptRecord()
			}
			cursor = value.Key
			if record.EnvironmentID != environmentID || !slices.Contains(kinds, record.Component.Desired.Kind) {
				continue
			}
			if record.Component.Desired.Enabled || len(record.Artifact) != 0 {
				clearRuntimePage(page)
				return nil, errs.New(errs.KindStateConflict, "A retained router runtime must be stopped before this Route can change without host execution")
			}
			conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
		}
		clearRuntimePage(page)
		if !page.More {
			return conditions, nil
		}
	}
}

func clearRuntimePage(page *keyvalue.RangeResult) {
	for index := range page.Values {
		clear(page.Values[index].Value)
	}
}
