package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// ReadFrozen reads only the operation-owned captured source. It does not
// resolve a Plan index, translate an old Task, or consult the live resource.
func ReadFrozen(ctx context.Context, store Reader, operationID, attachID string) (Input, int64, error) {
	key := FrozenKey(operationID, attachID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return Input{}, 0, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > read.ReadRevision {
		return Input{}, 0, recordcodec.CorruptRecord()
	}
	input, err := Decode(read.Values[0].Value)
	if err != nil {
		return Input{}, 0, err
	}
	if input.Attach.ID != attachID {
		Clear(&input)
		return Input{}, 0, recordcodec.CorruptRecord()
	}
	return input, read.Values[0].ModRevision, nil
}
