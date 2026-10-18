package app

import (
	"github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/materializationcontent"
)

func newComponentConfigurationCapture(store keyvalue.Store,
	catalog []componentrender.EnvironmentComponentRegistration,
) (*taskplanning.ComponentConfigurationCapture, error) {
	contents, err := materializationcontent.New(store)
	if err != nil {
		return nil, err
	}
	return taskplanning.NewComponentConfigurationCapture(catalog, contents)
}
