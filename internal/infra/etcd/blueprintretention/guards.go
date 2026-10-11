// Package blueprintretention protects immutable desired revisions while any
// current head, acknowledged runtime or unresolved execution still needs them.
package blueprintretention

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
}

// A revision remains one ownership unit: its chunks, effective projection,
// identities and prerequisites cannot expire while a head or execution needs it.
func Guards(
	ctx context.Context, store Store, environmentID, revisionID string, revision int64,
) ([]keyvalue.Condition, bool, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindTask, revisionID) != nil || revision <= 0 {
		return nil, false, errs.New(errs.KindInternal, "Blueprint revision retention owner is invalid")
	}
	keys := []string{
		blueprints.EnvironmentBlueprintHeadKey(environmentID),
		projectionrecord.EnvironmentComposeProjectionStorageKey(environmentID),
		blueprintunits.EpochKey(environmentID),
	}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, false, errs.New(errs.KindInternal, "Blueprint revision retention read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])}
	}
	if read.Values[1] != nil {
		applied, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[1].Value)
		if err != nil || applied.EnvironmentID != environmentID {
			return nil, false, errs.New(errs.KindInternal, "Blueprint revision applied owner is corrupt")
		}
		if applied.RevisionID == revisionID {
			return nil, true, nil
		}
	}
	if read.Values[0] != nil {
		headID, err := idempotency.DecodeTaskReference(read.Values[0].Value)
		if err != nil {
			return nil, false, err
		}
		if headID == revisionID {
			return nil, true, nil
		}
		key := blueprints.EnvironmentBlueprintOwnedIdentitiesKey(environmentID, headID)
		ownedRead, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
		if err != nil {
			return nil, false, err
		}
		if ownedRead == nil || ownedRead.ReadRevision != revision || len(ownedRead.Values) != 1 ||
			ownedRead.Values[0] == nil {
			return nil, false, errs.New(errs.KindInternal, "Blueprint revision current ownership is unavailable")
		}
		owned, decodeErr := projectionrecord.DecodeEnvironmentOwnedIdentities(ownedRead.Values[0].Value)
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: ownedRead.Values[0].ModRevision})
		keyvalue.ClearValues(ownedRead.Values)
		if decodeErr != nil || owned.EnvironmentID != environmentID || owned.RevisionID != headID {
			return nil, false, errs.New(errs.KindInternal, "Blueprint revision current ownership is corrupt")
		}
		for _, omission := range owned.PendingOmissions {
			if omission.SourceRevisionID != revisionID {
				continue
			}
			retired, err := omissionSourceRetired(ctx, store, environmentID, omission, revision)
			if err != nil || !retired {
				return nil, !retired, err
			}
			// Every receipt mutation advances the compared unit epoch, so a
			// large omission set needs no unbounded per-receipt comparisons.
		}
	}
	held, err := executionHeld(ctx, store, environmentID, revisionID, revision)
	return conditions, held, err
}

func omissionSourceRetired(
	ctx context.Context, store Store, environmentID string, omission projectionrecord.OwnedOmission, revision int64,
) (bool, error) {
	target := blueprintunits.ResourceKey{Kind: omission.Kind, ID: omission.ID}
	key := blueprintunits.AppliedKey(environmentID, target)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{key}, Revision: revision,
	})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return false, errs.New(errs.KindInternal, "Blueprint omission retention read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return false, nil
	}
	receipt, err := blueprintunits.DecodeApplied(read.Values[0].Value)
	if err != nil || receipt.EnvironmentID != environmentID || receipt.Target != target {
		return false, errs.New(errs.KindInternal, "Blueprint omission retention receipt is corrupt")
	}
	return receipt.State == blueprintunits.Absent &&
		(receipt.SourceTaskID != "" || receipt.ControllerOperationID != ""), nil
}

func executionHeld(
	ctx context.Context, store Store, environmentID, parentID string, revision int64,
) (bool, error) {
	prefix := blueprintunits.ExecutionPrefix(environmentID)
	start := ""
	for {
		page, err := store.Range(ctx, keyvalue.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: 64, Revision: revision,
		})
		if err != nil {
			return false, err
		}
		if page == nil || page.ReadRevision != revision {
			return false, errs.New(errs.KindInternal, "Blueprint revision execution retention read is incomplete")
		}
		more := page.More
		for _, value := range page.Values {
			execution, err := blueprintunits.DecodeExecution(value.Value)
			if err != nil || execution.EnvironmentID != environmentID ||
				value.Key != blueprintunits.ExecutionKey(environmentID, execution.PlanID) {
				keyvalue.ClearRangeValues(page.Values)
				return false, errs.New(errs.KindInternal, "Blueprint revision execution retention is corrupt")
			}
			if execution.ParentTaskID == parentID {
				keyvalue.ClearRangeValues(page.Values)
				return true, nil
			}
			start = value.Key
		}
		count := len(page.Values)
		keyvalue.ClearRangeValues(page.Values)
		if !more {
			return false, nil
		}
		if count == 0 {
			return false, errs.New(errs.KindInternal, "Blueprint revision execution retention cursor did not advance")
		}
	}
}
