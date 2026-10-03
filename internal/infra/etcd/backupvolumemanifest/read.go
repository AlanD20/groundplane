package backupvolumemanifest

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type cursorReader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

// ReadCursor verifies one exact manifest owner at a fixed MVCC read. Cleanup
// preparation needs this evidence without acquiring a manifest-writing port.
func ReadCursor(ctx context.Context, reader cursorReader, owner Owner,
	revision int64,
) (keyvalue.Versioned[Cursor], bool, error) {
	var zero keyvalue.Versioned[Cursor]
	if reader == nil || owner.Validate() != nil || revision < 0 {
		return zero, false, invalidLedger()
	}
	key := CursorKey(owner.Binding)
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return zero, false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 ||
		(revision > 0 && read.ReadRevision != revision) {
		return zero, false, invalidLedger()
	}
	zero.ReadRevision = read.ReadRevision
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return zero, false, nil
	}
	cursor, err := decodeCursor(value.Value)
	if err != nil || value.Key != key || value.ModRevision <= 0 || cursor.Owner != owner {
		return zero, false, invalidLedger()
	}
	return keyvalue.Versioned[Cursor]{Record: cursor, Revision: value.ModRevision,
		ReadRevision: read.ReadRevision}, true, nil
}
