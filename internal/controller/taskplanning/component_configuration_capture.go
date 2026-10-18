package taskplanning

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type componentConfigurationReader interface {
	Load(context.Context, taskmaterialization.Record, uint64) ([]byte, error)
}

// ComponentConfigurationCapture seals activation and recovery sources without
// consulting Task history or depending on the superseded execution planner.
type ComponentConfigurationCapture struct {
	componentCatalog []componentrender.EnvironmentComponentRegistration
	contents         componentConfigurationReader
}

func NewComponentConfigurationCapture(catalog []componentrender.EnvironmentComponentRegistration,
	contents componentConfigurationReader,
) (*ComponentConfigurationCapture, error) {
	if contents == nil {
		return nil, errs.New(errs.KindInternal, "Component configuration source reader is required")
	}
	if err := componentrender.ValidateEnvironmentComponentCatalog(catalog); err != nil {
		return nil, err
	}
	return &ComponentConfigurationCapture{
		componentCatalog: componentrender.CloneEnvironmentComponentCatalog(catalog), contents: contents,
	}, nil
}
