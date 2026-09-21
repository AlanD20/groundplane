package attachments

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type attachRemovalStore interface {
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}
