package acceptedactions

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CollectDrafts collects only expired, unpublished captures. It compares Task
// absence and the exact mutable descriptor revision, so racing final acceptance
// or a chunk batch wins or loses atomically; no accepted input can be collected.
func (repository *Repository) CollectDrafts(ctx context.Context, after string, at time.Time) (string, error) {
	if !recordcodec.IsCanonicalUTC(at) || after != "" && !strings.HasPrefix(after, expiryPrefix) {
		return "", errs.New(errs.KindValidationFailed, "accepted input collection cursor or time is invalid")
	}
	page, err := repository.store.Range(
		ctx,
		keyvalue.RangeRequest{Prefix: expiryPrefix, StartExclusive: after, Limit: 16},
	)
	if err != nil {
		return "", err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > 16 || page.More && len(page.Values) == 0 {
		return "", errs.New(errs.KindInternal, "accepted input collection page is incomplete")
	}
	last := after
	for _, entry := range page.Values {
		timestamp, id, ok := strings.Cut(strings.TrimPrefix(entry.Key, expiryPrefix), "/")
		expires, parseErr := time.Parse(timeLayout, timestamp)
		if !ok || parseErr != nil || ids.Validate(ids.KindTask, id) != nil || string(entry.Value) != id ||
			entry.Key <= last || entry.ModRevision <= 0 || entry.ModRevision > page.ReadRevision {
			return "", errs.New(errs.KindInternal, "accepted input collection index is corrupt")
		}
		if expires.After(at) {
			return "", nil
		}
		last = entry.Key
		read, err := repository.store.GetMany(
			ctx,
			keyvalue.GetManyRequest{Keys: []string{taskKey(id), Key(id)}, Revision: page.ReadRevision},
		)
		if err != nil {
			return "", err
		}
		if read == nil || read.ReadRevision != page.ReadRevision || len(read.Values) != 2 || read.Values[1] == nil {
			if read != nil {
				keyvalue.ClearValues(read.Values)
			}
			return "", errs.New(errs.KindInternal, "accepted input collection descriptor is missing")
		}
		if read.Values[0] != nil {
			keyvalue.ClearValues(read.Values)
			continue
		}
		manifest := read.Values[1]
		record, err := recordcodec.Decode[Record](manifest.Value, "accepted-action-input")
		if err != nil || record.Validate() != nil || record.Sealed || record.TaskID != id ||
			expiryKey(record) != entry.Key {
			keyvalue.ClearValues(read.Values)
			return "", errs.New(
				errs.KindInternal,
				"accepted input collection descriptor differs from its staging index",
			)
		}
		conditions := []keyvalue.Condition{{Key: taskKey(id)}, {Key: Key(id), ModRevision: manifest.ModRevision},
			{Key: entry.Key, ModRevision: entry.ModRevision}}
		mutations := []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: Prefix(id), Prefix: true},
			{Type: keyvalue.MutationDelete, Key: entry.Key}}
		keyvalue.ClearValues(read.Values)
		budget, err := repository.store.MeasureTransaction(ctx, conditions, mutations)
		if err != nil {
			return "", err
		}
		if !budget.Fits() {
			return "", errs.New(errs.KindInternal, "accepted input collection exceeds atomic storage limits")
		}
		// A losing compare means acceptance/staging won. Continue the page;
		// the next sweep rereads any remaining index without replaying writes.
		if _, err := repository.store.Transact(ctx, conditions, mutations); err != nil {
			return "", err
		}
	}
	if page.More {
		return last, nil
	}
	return "", nil
}
