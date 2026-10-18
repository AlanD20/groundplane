package workqueue

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type RetentionCandidate struct {
	Task TaskSnapshot
	Key  string
}

type RetentionPage struct {
	Candidates []RetentionCandidate
	Next       string
}

// Expired returns at most sixteen Tasks. The expiry index remains present while
// collection is in progress so restart needs neither an unbounded history scan
// nor an in-memory record of partially removed inputs.
func (repository *Repository) Expired(ctx context.Context, after string, at time.Time) (RetentionPage, error) {
	return repository.RetentionPageAt(ctx, after, at, 0)
}

// RetentionPageAt keeps maintenance/status readers on their selected snapshot.
// Candidate membership never grants collection; each operation checks its own
// actual deadline, protected inputs and recovery ownership before removal.
func (repository *Repository) RetentionPageAt(ctx context.Context, after string, at time.Time, revision int64) (RetentionPage, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return RetentionPage{}, err
	}
	if at.IsZero() || revision < 0 {
		return RetentionPage{}, invalid("Task retention requires a collection time")
	}
	if after != "" {
		if _, _, err := parseRetentionKey(after); err != nil {
			return RetentionPage{}, invalid("Task retention cursor is invalid")
		}
	}
	index, err := repository.store.Range(ctx, keyvalue.RangeRequest{Prefix: retentionPrefix, StartExclusive: after, Limit: 16, Revision: revision})
	if err != nil {
		return RetentionPage{}, err
	}
	if index == nil || index.ReadRevision <= 0 || revision > 0 && index.ReadRevision != revision || len(index.Values) > 16 || index.More && len(index.Values) == 0 {
		return RetentionPage{}, corrupt()
	}
	var page RetentionPage
	keys := make([]string, 0, len(index.Values))
	last := after
	reachedFuture := false
	for _, entry := range index.Values {
		id, until, err := parseRetentionKey(entry.Key)
		if err != nil || entry.Key <= last || string(entry.Value) != id || entry.ModRevision <= 0 || entry.ModRevision > index.ReadRevision {
			return RetentionPage{}, corrupt()
		}
		if until.After(at) {
			reachedFuture = true
			break
		}
		keys = append(keys, TaskKey(id))
		last = entry.Key
	}
	if index.More && !reachedFuture {
		page.Next = last
	}
	if len(keys) == 0 {
		return page, nil
	}
	read, err := repository.read(ctx, keys, index.ReadRevision)
	if err != nil {
		return RetentionPage{}, err
	}
	for offset, value := range read.Values {
		if value == nil {
			return RetentionPage{}, corrupt()
		}
		task, err := DecodeTask(value.Value)
		if err != nil || task.RetainUntil == nil || task.FinishedAt == nil ||
			TaskKey(task.ID) != keys[offset] || retentionKey(task.ID, *task.RetainUntil) != index.Values[offset].Key {
			return RetentionPage{}, corrupt()
		}
		page.Candidates = append(page.Candidates, RetentionCandidate{
			Task: TaskSnapshot{Task: task, Revision: value.ModRevision}, Key: index.Values[offset].Key,
		})
	}
	return page, nil
}

func parseRetentionKey(key string) (string, time.Time, error) {
	if !strings.HasPrefix(key, retentionPrefix) {
		return "", time.Time{}, corrupt()
	}
	stamp, id, found := strings.Cut(strings.TrimPrefix(key, retentionPrefix), "/")
	if !found || ids.Validate(ids.KindTask, id) != nil {
		return "", time.Time{}, corrupt()
	}
	until, err := time.Parse(retentionTimeLayout, stamp)
	if err != nil || retentionKey(id, until) != key {
		return "", time.Time{}, corrupt()
	}
	return id, until, nil
}
