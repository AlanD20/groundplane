package servicelifecyclework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *Repository) read(
	ctx context.Context,
	keys []string,
	revision int64,
) (*keyvalue.GetManyResult, error) {
	if revision <= 0 || len(keys) == 0 {
		return nil, conflict("Service lifecycle read requires its execution snapshot")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil {
		return nil, conflict("Service lifecycle execution read is unavailable")
	}
	valid := read.ReadRevision == revision && len(read.Values) == len(keys)
	if valid {
		for index, value := range read.Values {
			if value != nil && (value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision) {
				valid = false
				break
			}
		}
	}
	if !valid {
		keyvalue.ClearValues(read.Values)
		return nil, conflict("Service lifecycle execution read is inconsistent")
	}
	return read, nil
}
