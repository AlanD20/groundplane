package taskmaterialization

import (
	"context"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type componentMaterializationContentRepository interface {
	Stage(context.Context, materializationrecord.Record, uint64, []byte) error
	Load(context.Context, materializationrecord.Record, uint64) ([]byte, error)
}

func (resolver *TaskMaterializationResolver) EnableComponentMaterializationContent(
	repository componentMaterializationContentRepository,
) error {
	if resolver == nil || repository == nil {
		return errs.New(errs.KindInternal, "Component materialization content repository is required")
	}
	resolver.materializations = repository
	return nil
}

// RetainComponentFile persists only exact generated Component plain-file
// bytes. Blueprint and secret-derived sources remain in their owning stores.
func (resolver *TaskMaterializationResolver) RetainComponentFile(
	ctx context.Context,
	record materializationrecord.Record,
	generation uint64,
	content []byte,
) error {
	if resolver == nil || resolver.materializations == nil {
		return errs.New(errs.KindInternal, "Component materialization content repository is unavailable")
	}
	return resolver.materializations.Stage(ctx, record, generation, content)
}
