// Package runneraccessplatform composes the isolated Runner API.
package runneraccessplatform

import (
	"log/slog"

	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	"github.com/AlanD20/groundplane/internal/controller/releaseoperation"
	"github.com/AlanD20/groundplane/internal/controller/runneridentity"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnercredential"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnergrants"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerscoped"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type Platform struct {
	Handler     *runneridentity.Handler
	Credentials *runnercredential.Repository
	Grants      *runneridentity.GrantManager
}

func New(
	store keyvalue.Store, queue *workqueue.Repository, releases *releaseoperation.Service,
	images *imagedelivery.Service, logger *slog.Logger,
) (*Platform, error) {
	credentials, err := runnercredential.New(store)
	if err != nil {
		return nil, err
	}
	grants := runnergrants.New(store)
	admission, err := runneridentity.NewAdmission(
		store,
		runners.NewReader(store),
		queue,
		credentials,
		releases,
		images,
		runneridentity.NewStoredGrantReader(grants),
	)
	if err != nil {
		return nil, err
	}
	handler, err := runneridentity.New(credentials, admission, logger)
	if err != nil {
		return nil, err
	}
	return &Platform{
		Handler: handler, Credentials: credentials,
		Grants: runneridentity.NewGrantManager(admission, grants, credentials, runnerscoped.NewRepository(store)),
	}, nil
}
