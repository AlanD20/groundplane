package host

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SystemSnapshot contains only accepted Host fields. Formatting remains the
// source's responsibility until the live unit and rounding contract is locked.
type SystemSnapshot struct {
	Hostname string
	Arch     string
	OS       string
	Uptime   string
	CPU      api.HostCPU
	Memory   api.HostResource
	Disk     api.HostResource
	Swap     api.HostResource
	Docker   string
}

// EtcdSnapshot is the safe host-level etcd projection. Expected unavailability
// is represented by StatusFailed, not an error or a raw endpoint diagnostic.
type EtcdSnapshot struct {
	Node   string
	Status api.HealthState
	DBSize string
}

// AgentSnapshot is the non-secret local Agent projection. Credential and
// transport state cannot be represented by this port.
type AgentSnapshot struct {
	Status        api.HealthState
	PullInterval  string
	MaxConcurrent int
	Labels        []string
}

type SystemSnapshotSource interface {
	SystemSnapshot(ctx context.Context) (SystemSnapshot, error)
}

type EtcdSnapshotSource interface {
	EtcdSnapshot(ctx context.Context) (EtcdSnapshot, error)
}

type AgentSnapshotSource interface {
	AgentSnapshot(ctx context.Context) (AgentSnapshot, error)
}

type ControllerUpdateSource interface {
	ControllerUpdateSnapshot(context.Context) (api.ControllerUpdateState, error)
}

type HostDependencies struct {
	System            SystemSnapshotSource
	Etcd              EtcdSnapshotSource
	Agent             AgentSnapshotSource
	Updates           ControllerUpdateSource
	ControllerService string
	ControllerVersion string
}

// HostService projects three independently injected observations into the one
// accepted Host response. It owns error normalization and never exposes source
// errors, etcd endpoints, or Agent credential material.
type HostService struct {
	system            SystemSnapshotSource
	etcd              EtcdSnapshotSource
	agent             AgentSnapshotSource
	updates           ControllerUpdateSource
	controllerService string
	controllerVersion string
}

func NewHostService(dependencies HostDependencies) (*HostService, error) {
	if dependencies.System == nil {
		return nil, errs.New(errs.KindInternal, "host system snapshot source is required")
	}
	if dependencies.Etcd == nil {
		return nil, errs.New(errs.KindInternal, "host etcd snapshot source is required")
	}
	if dependencies.Agent == nil {
		return nil, errs.New(errs.KindInternal, "host agent snapshot source is required")
	}
	if dependencies.Updates == nil {
		return nil, errs.New(errs.KindInternal, "host Controller update source is required")
	}
	if dependencies.ControllerService == "" {
		return nil, errs.New(errs.KindInternal, "host controller service is required")
	}
	if dependencies.ControllerVersion == "" {
		return nil, errs.New(errs.KindInternal, "host controller version is required")
	}
	return &HostService{
		system:            dependencies.System,
		etcd:              dependencies.Etcd,
		agent:             dependencies.Agent,
		updates:           dependencies.Updates,
		controllerService: dependencies.ControllerService,
		controllerVersion: dependencies.ControllerVersion,
	}, nil
}

func (service *HostService) Show(ctx context.Context) (api.Host, error) {
	if ctx == nil {
		return api.Host{}, errs.New(errs.KindInternal, "host context is required")
	}
	if err := ctx.Err(); err != nil {
		return api.Host{}, err
	}

	system, err := service.system.SystemSnapshot(ctx)
	if err != nil {
		return api.Host{}, hostSourceError(ctx, err)
	}
	etcd, err := service.etcd.EtcdSnapshot(ctx)
	if err != nil {
		return api.Host{}, hostSourceError(ctx, err)
	}
	agent, err := service.agent.AgentSnapshot(ctx)
	if err != nil {
		return api.Host{}, hostSourceError(ctx, err)
	}
	if !validHealthState(etcd.Status) || !validHealthState(agent.Status) {
		return api.Host{}, errs.New(errs.KindInternal, "host source returned an invalid health state")
	}
	updates, err := service.updates.ControllerUpdateSnapshot(ctx)
	if err != nil {
		return api.Host{}, hostSourceError(ctx, err)
	}

	return api.Host{
		Hostname: system.Hostname,
		Arch:     system.Arch,
		OS:       system.OS,
		Uptime:   system.Uptime,
		CPU:      system.CPU,
		Memory:   system.Memory,
		Disk:     system.Disk,
		Swap:     system.Swap,
		Docker:   system.Docker,
		Etcd: api.HostEtcd{
			Node:   etcd.Node,
			Status: etcd.Status,
			DBSize: etcd.DBSize,
		},
		Controller: api.HostController{
			Service: service.controllerService,
			Status:  api.HealthHealthy,
			Version: service.controllerVersion,
			Update:  updates,
		},
		Agent: api.HostAgent{
			Status:        agent.Status,
			PullInterval:  agent.PullInterval,
			MaxConcurrent: agent.MaxConcurrent,
			Labels:        append(make([]string, 0, len(agent.Labels)), agent.Labels...),
		},
	}, nil
}

func hostSourceError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		return domainError
	}
	return errs.Wrap(errs.KindInternal, err)
}

func validHealthState(status api.HealthState) bool {
	switch status {
	case api.HealthHealthy, api.HealthDegraded, api.HealthFailed, api.HealthStopped, api.HealthPending:
		return true
	default:
		return false
	}
}
