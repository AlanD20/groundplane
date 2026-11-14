package secrets

import (
	"github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// MutationService composes the two Secret writes without exposing repository or
// encryption details to the transport and application wiring layers.
type MutationService struct {
	*secretCreationService
	*ValueEditor
}

func NewMutationService(
	repository *Repository,
	protector *secretvalue.Protector,
	evidence *etcd.IdempotencyRepository,
	coordinator *idempotency.Coordinator,
) (*MutationService, error) {
	creationEvidence, err := NewCreationIdempotency(coordinator, evidence)
	if err != nil {
		return nil, err
	}
	creation, err := NewCreationService(repository, protector, creationEvidence)
	if err != nil {
		return nil, err
	}
	editor, err := NewValueEditor(repository, protector, evidence, coordinator)
	if err != nil {
		return nil, err
	}
	return &MutationService{secretCreationService: creation, ValueEditor: editor}, nil
}
