package etcd

import (
	"bytes"
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ListPlanningHookScriptIDs returns the fixed-revision hook selection for one
// release member in deterministic numeric order then scoped slug.
func (ledger *ReleaseLedger) ListPlanningHookScriptIDs(
	ctx context.Context,
	scope ReleasePlanningScope,
	serviceID string,
	operation domain.OperationKind,
) ([]string, error) {
	if ctx == nil || ledger == nil || ledger.store == nil || scope.ReadRevision <= 0 ||
		ids.Validate(ids.KindService, serviceID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "release hook selection is invalid")
	}
	allowed := map[core.ScriptHook]struct{}{core.ScriptOnFailure: {}}
	if operation == domain.OperationDeploy {
		allowed[core.ScriptPreDeploy], allowed[core.ScriptPostDeploy] = struct{}{}, struct{}{}
	} else if operation == domain.OperationRollback {
		allowed[core.ScriptPreRollback], allowed[core.ScriptPostRollback] = struct{}{}, struct{}{}
	} else {
		return nil, errs.New(errs.KindValidationFailed, "release hook operation is invalid")
	}
	selected := make([]core.Script, 0)
	active, err := readActiveScriptSet(ctx, ledger.store, scope.Environment.Record.ID, scope.ReadRevision)
	if err != nil {
		return nil, err
	}
	prefix, start := scriptrecord.ScriptSetOwnerPrefix(scope.Environment.Record.ID, active.Record.GenerationID), ""
	for {
		page, err := ledger.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: 64, Revision: scope.ReadRevision,
		})
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision != scope.ReadRevision {
			return nil, releases.CorruptReleaseRecord()
		}
		for _, value := range page.Values {
			scriptID := strings.TrimPrefix(value.Key, prefix)
			if strings.Contains(scriptID, "/") || ids.Validate(ids.KindScript, scriptID) != nil ||
				!bytes.Equal(value.Value, []byte(scriptID)) {
				return nil, releases.CorruptReleaseRecord()
			}
			primary, readErr := scriptExecutionValueAt(ctx, ledger.store, scriptrecord.ScriptSetScriptKey(
				scope.Environment.Record.ID, active.Record.GenerationID, scriptID,
			), scope.ReadRevision)
			if readErr != nil {
				return nil, readErr
			}
			record, decodeErr := scriptrecord.DecodeRecord(primary.Value)
			if decodeErr != nil || record.Desired.ID != scriptID ||
				record.EnvironmentID != scope.Environment.Record.ID ||
				record.ScriptSetGeneration != active.Record.GenerationID {
				return nil, releases.CorruptReleaseRecord()
			}
			if record.ServiceID == serviceID {
				if _, exists := allowed[record.Desired.When]; exists {
					selected = append(selected, record.Desired)
				}
			}
		}
		if len(page.Values) == 0 || !page.More {
			break
		}
		start = page.Values[len(page.Values)-1].Key
	}
	sort.Slice(selected, func(i, j int) bool { return core.ScriptBefore(selected[i], selected[j]) })
	result := make([]string, len(selected))
	for index := range selected {
		result[index] = selected[index].ID
	}
	return result, nil
}
