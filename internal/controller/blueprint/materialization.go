package blueprint

import (
	"context"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
)

type materializationResolver interface {
	PinSecretValue(context.Context, string, string) (materializationrecord.SecretValueReference, error)
	RetainComponentFile(context.Context, materializationrecord.Record, uint64, []byte) error
	ResolveTaskMaterializationSource(
		context.Context,
		string,
		materializationrecord.Source,
	) ([]byte, error)
}
