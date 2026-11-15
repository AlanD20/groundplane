// Package serviceterminal owns container-shell target capture. The shared
// terminal manager owns session admission and browser lifetime, not Service state.
package serviceterminal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/terminalsession"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/hostterminal"
	"github.com/AlanD20/groundplane/internal/controller/serviceruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ServiceReader interface {
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
}

type Service struct {
	services ServiceReader
	releases *etcd.ReleaseLedger
	agents   *agentchannel.Registry
	sessions *hostterminal.Manager
}

func New(
	services ServiceReader,
	releases *etcd.ReleaseLedger,
	agents *agentchannel.Registry,
	sessions *hostterminal.Manager,
) *Service {
	return &Service{services: services, releases: releases, agents: agents, sessions: sessions}
}

func (s *Service) Open(
	ctx context.Context,
	owner, key, serviceID string,
	options apiTypes.ContainerTerminalOpen,
) (apiTypes.TerminalSession, string, error) {
	if s == nil || s.services == nil || s.releases == nil || s.agents == nil || s.sessions == nil {
		return apiTypes.TerminalSession{}, "", errs.New(
			errs.KindRequestUnavailable,
			"container terminal is unavailable",
		)
	}
	if options.User == "" {
		options.User = "root"
	}
	record, err := s.services.GetService(ctx, serviceID)
	if err != nil {
		return apiTypes.TerminalSession{}, "", err
	}
	sources, err := serviceruntime.CaptureSources(ctx, s.releases, record)
	if err != nil {
		return apiTypes.TerminalSession{}, "", err
	}
	if len(sources) == 0 {
		return apiTypes.TerminalSession{}, "", errs.New(
			errs.KindStateConflict,
			"Service has no captured runtime; deploy it before opening a shell",
		)
	}
	target := hostterminal.Target{Kind: "container", ServiceID: serviceID, ContainerID: options.ContainerID,
		User: options.User, Command: append([]string(nil), options.Command...), WorkingDirectory: options.WorkingDirectory,
		Columns: uint16(options.Columns), Rows: uint16(options.Rows)}
	return s.sessions.OpenProcess(
		ctx,
		owner,
		key,
		target,
		func(ctx context.Context, id string) (terminalsession.Process, error) {
			return s.agents.OpenContainerShell(ctx, &agentpb.ContainerTerminalOpen{
				SessionId: id, Sources: sources, ContainerId: options.ContainerID, User: options.User, Command: options.Command,
				WorkingDirectory: options.WorkingDirectory, Columns: uint32(options.Columns), Rows: uint32(options.Rows),
				InputInactivitySeconds: uint32(s.sessions.InputInactivitySeconds()), InitialCredit: 64,
			})
		},
	)
}
