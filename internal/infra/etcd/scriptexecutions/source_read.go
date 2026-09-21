package scriptexecutions

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func ScriptExecutionValueAt(
	ctx context.Context,
	store sourceReadStore,
	key string,
	revision int64,
) (*etcdstore.KeyValue, error) {
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "Script execution source is missing at its fixed revision")
	}
	return read.Values[0], nil
}
