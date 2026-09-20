package operations

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type materializationResolver interface {
	PinSecretValue(context.Context, string, string) (etcd.TaskSecretValueReference, error)
	RetainComponentFile(context.Context, etcd.TaskMaterializationRecord, uint64, []byte) error
	ResolveTaskMaterializationSource(context.Context, string, etcd.TaskMaterializationSource) ([]byte, error)
}
