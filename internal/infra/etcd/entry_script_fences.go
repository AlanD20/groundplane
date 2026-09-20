package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Entry removal deletes all immutable generations, not just the current one.
// Prefix absence proves that neither accounting family retains any generation.
func entryScriptAbsenceConditions(entryID string) []etcdstore.Condition {
	suffix := "entry-value/" + entryID + "/"
	return []etcdstore.Condition{
		{Key: scriptSourceCountPrefix + suffix, Prefix: true},
		{Key: scriptSourceForwardReferencePrefix + suffix, Prefix: true},
	}
}

func classifyEntryScriptReferences(entryID string, values []*etcdstore.KeyValue) error {
	if len(values) != 2 || (values[0] == nil) != (values[1] == nil) {
		return errs.New(errs.KindInternal, "Entry Script reference absence proof is inconsistent")
	}
	if values[0] == nil {
		return nil
	}
	count, err := decodeScriptSourceCount(values[0].Value)
	if err != nil || count.Source.Kind != ScriptSourceEntryValue || count.Source.EntryID != entryID ||
		values[0].Key != scriptSourceCountKey(count.Source) || count.ReferencedExecutionCount == 0 {
		return errs.New(errs.KindInternal, "Entry Script reference count is corrupt")
	}
	reference, err := decodeEnvelope[ScriptSourceReference](values[1].Value, "script-source-reference")
	if err != nil || reference.Source != count.Source || scriptSourceForwardReferenceKey(reference) != values[1].Key {
		return errs.New(errs.KindInternal, "Entry Script source membership is corrupt")
	}
	return errs.New(errs.KindResourceInUse, "Entry is referenced by a Script")
}

func prepareEntryScriptAbsence(
	ctx context.Context, store hierarchyStore, entryID string, readRevision int64,
) ([]etcdstore.Condition, error) {
	conditions := entryScriptAbsenceConditions(entryID)
	values := make([]*etcdstore.KeyValue, 2)
	for index, condition := range conditions {
		result, err := store.Range(ctx, etcdstore.RangeRequest{Prefix: condition.Key, Limit: 1, Revision: readRevision})
		if err != nil {
			return nil, err
		}
		if result == nil || result.ReadRevision <= 0 ||
			(readRevision > 0 && result.ReadRevision != readRevision) || len(result.Values) > 1 ||
			(result.More && len(result.Values) == 0) {
			return nil, errs.New(errs.KindInternal, "Entry Script reference evidence is incomplete")
		}
		readRevision = result.ReadRevision
		if len(result.Values) == 1 {
			values[index] = &result.Values[0]
		}
	}
	if err := classifyEntryScriptReferences(entryID, values); err != nil {
		return nil, err
	}
	return conditions, nil
}

func classifyEntryScriptAbsenceConflict(
	entryIDs []string, baseCount int, base idempotencyPlanClassifier,
) idempotencyPlanClassifier {
	return func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != baseCount+2*len(entryIDs) {
			return errs.New(errs.KindInternal, "Entry Script compare evidence is incomplete")
		}
		for index, entryID := range entryIDs {
			start := baseCount + 2*index
			if err := classifyEntryScriptReferences(entryID, values[start:start+2]); err != nil {
				return err
			}
		}
		return base(revision, values[:baseCount])
	}
}
