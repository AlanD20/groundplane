package runneraccessplatform

import (
	"context"
	"crypto/tls"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/controller/buildcredentials"
	"github.com/AlanD20/groundplane/internal/controller/gitdeliveryoperator"
	"github.com/AlanD20/groundplane/internal/controller/handlers"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerscoped"
	"github.com/AlanD20/groundplane/internal/infra/githubapp"
	"github.com/AlanD20/groundplane/internal/infra/runnerregistry"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type GitHubAuthority struct {
	repository *runnerscoped.Repository
	api        string
	registry   string
	ready      func() bool
}

func NewGitHubAuthority(store keyvalue.Store, api, registry string, ready func() bool) GitHubAuthority {
	return GitHubAuthority{repository: runnerscoped.NewRepository(store), api: api, registry: registry, ready: ready}
}

func (authority GitHubAuthority) GitHubDeliveryAuthority(
	ctx context.Context, runnerID string,
) (gitdeliveryoperator.RunnerAuthorityEvidence, error) {
	if !authority.ready() {
		return gitdeliveryoperator.RunnerAuthorityEvidence{}, errs.New(
			errs.KindStateConflict,
			"Scoped Runner listeners are not ready",
		)
	}
	active, err := authority.repository.Authority(ctx, runnerID)
	if err != nil {
		return gitdeliveryoperator.RunnerAuthorityEvidence{}, err
	}
	if active.API.String() != authority.api || active.Registry.String() != authority.registry {
		return gitdeliveryoperator.RunnerAuthorityEvidence{}, errs.New(
			errs.KindStateConflict, "Runner was scoped for different API or registry addresses",
		)
	}
	return gitdeliveryoperator.RunnerAuthorityEvidence{GitHubID: active.GitHubID, Conditions: active.Conditions}, nil
}

func (platform *Platform) GitHubListeners(
	ctx context.Context,
	store keyvalue.Store,
	jobs buildcredentials.JobAuthority,
	apiAddress, registryAddress string,
) (*handlers.RunnerListener, *handlers.RunnerListener, error) {
	runner := &handlers.RunnerListener{Address: apiAddress, Handler: platform.Handler}
	if registryAddress == "" {
		return runner, nil, nil
	}
	endpoint, err := netip.ParseAddrPort(registryAddress)
	if err != nil {
		return nil, nil, err
	}
	exchange, err := buildcredentials.New(store, platform.Credentials, jobs, githubapp.NewOIDCVerifier(), endpoint)
	if err != nil {
		return nil, nil, err
	}
	gateway, certificate, err := runnerregistry.NewLocalGateway(ctx, endpoint, exchange)
	if err != nil {
		return nil, nil, err
	}
	runner.Handler = platform.Handler.WithRegistryCredentialExchange(exchange)
	registry := &handlers.RunnerListener{Address: registryAddress, Handler: gateway,
		TLS: &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}},
	}
	return runner, registry, nil
}
