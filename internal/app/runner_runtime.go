package app

import (
	"log/slog"
	"net/netip"
	"slices"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/config"
	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	dockerrunner "github.com/AlanD20/groundplane/internal/infra/docker/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/runnerjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const runnerJournalRoot = "/var/lib/groundplane/runner-journal"

func newRunnerLifecycleExecutor(
	logger *slog.Logger,
	repository *etcd.RunnerRepository,
	broker *runnercapability.TokenBroker,
	cfg config.ControllerConfig,
	pools config.AllocationPools,
) (*runnercapability.Executor, error) {
	denied := []netip.Prefix{pools.Environment, pools.System}
	var endpoint netip.AddrPort
	for _, address := range cfg.Listen.HTTP {
		current, err := netip.ParseAddrPort(address)
		if err != nil || !current.Addr().Is4() {
			return nil, errs.New(errs.KindInternal, "Runner Controller endpoint must be an IPv4 address")
		}
		if current.Addr() == netip.MustParseAddr("127.0.0.1") {
			endpoint = current
		}
		if !slices.ContainsFunc(denied, func(prefix netip.Prefix) bool {
			return prefix.Contains(current.Addr())
		}) {
			denied = append(denied, netip.PrefixFrom(current.Addr(), 32))
		}
	}
	if !endpoint.IsValid() {
		return nil, errs.New(errs.KindInternal, "Runner Controller loopback endpoint is missing")
	}
	sort.Slice(denied, func(left, right int) bool {
		return denied[left].Addr().Compare(denied[right].Addr()) < 0
	})

	host, err := dockerrunner.NewLocal(commandrunner.New(logger))
	if err != nil {
		return nil, err
	}
	journal, err := runnerjournal.New(runnerJournalRoot)
	if err != nil {
		return nil, err
	}
	lifecycle, err := runnercapability.NewLifecycle(journal, host)
	if err != nil {
		return nil, err
	}
	return runnercapability.NewExecutor(
		repository,
		lifecycle,
		broker,
		runnerallocation.RunnerAllocationConfigFromPools(pools),
		corerunner.IsolationPolicy{
			RunnerPool: pools.Runner.Network, DeniedCIDRs: denied, ControllerEndpoint: endpoint,
		},
	)
}
