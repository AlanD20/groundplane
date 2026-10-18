package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/controller/backingchecks"
	"github.com/AlanD20/groundplane/internal/controller/connectors"
	"github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	"github.com/AlanD20/groundplane/internal/controller/edgechecks"
	backingrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	edgerecord "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
)

type connectionWork struct {
	connectorRecords *connectorchecks.Repository
	backingRecords   *backingrecord.Repository
	dnsRecords       *dnschecks.Repository
	edgeRecords      *edgerecord.Repository
	connectors       *connectors.CheckExecutor
	backings         *backingchecks.Executor
	dns              *dnsresolver.ConnectionCheckExecutor
	edges            *edgechecks.Executor
}

func (platform *Platform) newConnectionWork() (*connectionWork, error) {
	checkRecords, err := connectorchecks.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	checks, err := connectors.NewCheckExecutor(checkRecords, platform.dependencies.Protector)
	if err != nil {
		return nil, err
	}
	backingRecords, err := backingrecord.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	backing, err := backingchecks.NewExecutor(backingRecords, platform.dependencies.Channel.Registry, platform.dependencies.Protector)
	if err != nil {
		return nil, err
	}
	dnsRecords, err := dnschecks.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	dns, err := dnsresolver.NewConnectionCheckExecutor(dnsRecords, platform.dependencies.Channel.Registry)
	if err != nil {
		return nil, err
	}
	edgeRecords, err := edgerecord.NewRepository(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	edge, err := edgechecks.NewExecutor(edgeRecords, platform.dependencies.Channel.Registry)
	if err != nil {
		return nil, err
	}
	return &connectionWork{connectorRecords: checkRecords, backingRecords: backingRecords, dnsRecords: dnsRecords, edgeRecords: edgeRecords,
		connectors: checks, backings: backing, dns: dns, edges: edge}, nil
}
