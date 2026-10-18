package app

import (
	"github.com/AlanD20/groundplane/internal/controller/componentrender"
	networkcontroller "github.com/AlanD20/groundplane/internal/controller/network"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/materializationcontent"
	networkstore "github.com/AlanD20/groundplane/internal/infra/etcd/network"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
)

func newNetworkRoutePublisher(store keyvalue.Store, hierarchy *etcd.HierarchyRepository,
	desired *desiredrevision.Repository, state *networkstore.Repository,
	catalog []componentrender.EnvironmentComponentRegistration,
) (*networkcontroller.RouteWorkPublisher, error) {
	providers, err := taskplanning.NewRouteProviderCapture(state, catalog)
	if err != nil {
		return nil, err
	}
	contents, err := materializationcontent.New(store)
	if err != nil {
		return nil, err
	}
	configuration, err := taskplanning.NewComponentConfigurationCapture(catalog, contents)
	if err != nil {
		return nil, err
	}
	snapshots, err := runtimeconfiguration.New(store)
	if err != nil {
		return nil, err
	}
	return networkcontroller.NewRouteWorkPublisher(hierarchy, desired, providers, configuration, snapshots, contents)
}
