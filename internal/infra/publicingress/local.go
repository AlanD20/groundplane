package publicingress

import (
	"context"
	"net/netip"
	"os"
	"path/filepath"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
	"golang.org/x/sys/unix"
)

// AgentCompanionBase and the activation directory live in the managed Agent's
// existing host state mount. Neither is part of the primary config document.
const AgentCompanionBase = "/var/lib/groundplane/agent/agent.yaml"
const localStateRoot = "/var/lib/groundplane/agent/public-ingress"

type Local struct {
	config config.PublicIngressConfig
	runner runner.Runner
}

func NewLocal(cfg config.PublicIngressConfig, taskRunner runner.Runner) (*Local, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if taskRunner == nil {
		return nil, errs.New(errs.KindInternal, "public ingress requires a bounded local command runner")
	}
	return &Local{config: cfg, runner: taskRunner}, nil
}

func (local *Local) Apply(ctx context.Context, plan ingressexecution.Plan) (ingressexecution.Report, error) {
	options, err := local.plan(ctx, plan)
	if err != nil {
		return ingressexecution.Report{}, err
	}
	if err := verifyLocalAddresses(options.Listeners); err != nil {
		return ingressexecution.Report{}, err
	}
	origins, err := routeOrigins(plan.Configuration)
	if err != nil {
		return ingressexecution.Report{}, err
	}
	engine, err := client.New(client.WithHost(hostDockerSocket))
	if err != nil {
		return ingressexecution.Report{}, err
	}
	defer engine.Close()
	bridge, err := inspectBridge(ctx, engine, options.HostID, options.NetworkID)
	if err != nil {
		return ingressexecution.Report{}, err
	}
	if err := local.verifyRelayBridge(ctx, engine, bridge); err != nil {
		return ingressexecution.Report{}, err
	}
	image, err := engine.ImageInspect(ctx, options.Image)
	if err != nil {
		return ingressexecution.Report{}, provisionError(ctx, "verify the pinned Caddy image", err)
	}
	if err := local.verifyEdgeMembership(ctx, engine, bridge, options.HostID, image.ID, options.Listeners); err != nil {
		return ingressexecution.Report{}, err
	}
	if err := checkRelayOrigins(ctx, local.config.Relay.Listen(), origins); err != nil {
		return ingressexecution.Report{}, err
	}
	if plan.Predecessor != nil {
		return local.applyExisting(ctx, plan, options)
	}
	if err := prepareLocalState(options.StateDirectory); err != nil {
		return ingressexecution.Report{}, err
	}
	if err := local.ensureRelay(ctx, engine, options.StateDirectory); err != nil {
		return ingressexecution.Report{}, err
	}
	identity, err := Provision(ctx, options)
	if err != nil {
		return ingressexecution.Report{}, err
	}
	return local.bootstrap(ctx, plan, identity)
}

func (local *Local) applyExisting(ctx context.Context, plan ingressexecution.Plan,
	options ProvisionOptions,
) (ingressexecution.Report, error) {
	identity, err := inspectInstallation(ctx, options, plan.Predecessor.RuntimeID)
	if err != nil {
		return ingressexecution.Report{}, err
	}
	engine, err := client.New(client.WithHost(hostDockerSocket))
	if err != nil {
		return ingressexecution.Report{}, err
	}
	defer engine.Close()
	if _, err := local.inspectRelay(ctx, engine, options.StateDirectory, true); err != nil {
		return ingressexecution.Report{}, err
	}
	if err := probeTCP(ctx, local.config.Relay.Listen()); err != nil {
		return ingressexecution.Report{}, err
	}
	store, err := local.store(identity)
	if err != nil {
		return ingressexecution.Report{}, err
	}
	config := Configuration{RuntimeID: identity.RuntimeID, Revision: plan.Revision, JSON: plan.Configuration}
	receipt, err := store.Apply(ctx, Receipt(*plan.Predecessor), config)
	if err != nil {
		// A failed candidate can have restored the exact predecessor. Evidence
		// after the attempt, rather than a particular error string, settles it.
		if observed, observationErr := store.Observe(ctx, Receipt(*plan.Predecessor)); observationErr == nil {
			return localReport(plan, observed)
		}
		return ingressexecution.Report{}, err
	}
	return localReport(plan, receipt)
}

func (local *Local) Observe(ctx context.Context, plan ingressexecution.Plan) (ingressexecution.Report, error) {
	return local.readOrRecover(ctx, plan, false)
}

func (local *Local) Recover(ctx context.Context, plan ingressexecution.Plan) (ingressexecution.Report, error) {
	return local.readOrRecover(ctx, plan, true)
}

func (local *Local) readOrRecover(ctx context.Context, plan ingressexecution.Plan, recover bool,
) (ingressexecution.Report, error) {
	options, err := local.plan(ctx, plan)
	if err != nil {
		return ingressexecution.Report{}, err
	}
	expectedID := ""
	if plan.Predecessor != nil {
		expectedID = plan.Predecessor.RuntimeID
	}
	identity, err := inspectInstallation(ctx, options, expectedID)
	if err != nil {
		return ingressexecution.Report{}, err
	}
	if plan.Predecessor == nil {
		return local.bootstrap(ctx, plan, identity)
	}
	store, err := local.store(identity)
	if err != nil {
		return ingressexecution.Report{}, err
	}
	predecessor := Receipt(*plan.Predecessor)
	candidate := Receipt{RuntimeID: identity.RuntimeID, Revision: plan.Revision, Digest: plan.Digest}
	if recover {
		receipt, err := store.Recover(ctx, predecessor, candidate)
		if err == nil {
			return localReport(plan, receipt)
		}
		// Recover may restore the previous known-good configuration and report
		// the candidate's failure. Only a matching live receipt proves restore.
		if observed, observationErr := store.Observe(ctx, predecessor); observationErr == nil {
			return localReport(plan, observed)
		}
		return ingressexecution.Report{}, err
	}
	receipt, err := store.Observe(ctx, predecessor, candidate)
	if err != nil {
		return ingressexecution.Report{}, err
	}
	return localReport(plan, receipt)
}

func (local *Local) bootstrap(ctx context.Context, plan ingressexecution.Plan, identity DockerIdentity,
) (ingressexecution.Report, error) {
	store, err := local.store(identity)
	if err != nil {
		return ingressexecution.Report{}, err
	}
	receipt, err := store.Bootstrap(ctx, Configuration{
		RuntimeID: identity.RuntimeID, Revision: plan.Revision, JSON: plan.Configuration,
	})
	if err != nil {
		return ingressexecution.Report{}, err
	}
	return localReport(plan, receipt)
}

func (local *Local) plan(ctx context.Context, plan ingressexecution.Plan) (ProvisionOptions, error) {
	if ctx == nil {
		return ProvisionOptions{}, conflict("public ingress requires an Agent context")
	}
	if err := ctx.Err(); err != nil {
		return ProvisionOptions{}, err
	}
	if err := plan.Validate(); err != nil {
		return ProvisionOptions{}, err
	}
	if plan.HostID != local.config.HostID {
		return ProvisionOptions{}, conflict("public ingress assignment belongs to a different Host")
	}
	if _, err := caddyConfiguration(plan.Configuration); err != nil {
		return ProvisionOptions{}, err
	}
	return ProvisionOptions{
		StateDirectory: filepath.Join(localStateRoot, plan.HostID), Image: local.config.Image,
		NetworkID: local.config.Bridge, HostID: plan.HostID, Listeners: localListeners(plan.Addresses),
		InitialJSON: plan.Configuration, EdgeAddress: local.config.Relay.EdgeIP,
	}, nil
}

func localListeners(addresses []netip.Addr) []netip.AddrPort {
	listeners := make([]netip.AddrPort, 0, 2*len(addresses))
	for _, address := range addresses {
		listeners = append(listeners, netip.AddrPortFrom(address, 80), netip.AddrPortFrom(address, 443))
	}
	return listeners
}

func localReport(plan ingressexecution.Plan, receipt Receipt) (ingressexecution.Report, error) {
	identity := ingressexecution.Receipt(receipt)
	if !identity.Valid() {
		return ingressexecution.Report{}, conflict("public ingress runtime returned invalid evidence")
	}
	if receipt.Revision == plan.Revision && receipt.Digest == plan.Digest &&
		(plan.Predecessor == nil || receipt.RuntimeID == plan.Predecessor.RuntimeID) {
		return ingressexecution.Report{Result: ingressexecution.Applied, Receipt: &identity}, nil
	}
	if plan.Predecessor != nil && identity == *plan.Predecessor {
		return ingressexecution.Report{Result: ingressexecution.Restored, Receipt: &identity}, nil
	}
	return ingressexecution.Report{}, conflict("public ingress runtime returned unrelated evidence")
}

func inspectInstallation(ctx context.Context, options ProvisionOptions, expectedID string) (DockerIdentity, error) {
	engine, err := client.New(client.WithHost(hostDockerSocket))
	if err != nil {
		return DockerIdentity{}, err
	}
	defer engine.Close()
	image, err := engine.ImageInspect(ctx, options.Image)
	if err != nil {
		return DockerIdentity{}, provisionError(ctx, "verify Caddy image", err)
	}
	name := edgeName(options.HostID)
	actual, err := engine.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
	if err != nil {
		return DockerIdentity{}, provisionError(ctx, "observe the owned edge", err)
	}
	identity := DockerIdentity{
		RuntimeID: actual.Container.ID, ImageID: image.ID, HostID: options.HostID, NetworkID: options.NetworkID,
		StateDirectory: options.StateDirectory, CertVolume: name + "-tls", Listeners: options.Listeners,
		EdgeAddress: options.EdgeAddress,
	}
	if !validDockerIdentity(identity) || expectedID != "" && expectedID != identity.RuntimeID ||
		!matchesInstallation(actual.Container, edgeContainerOptions(name, identity), identity) ||
		!actual.Container.State.Running {
		return DockerIdentity{}, conflict("public ingress runtime differs from its captured installation")
	}
	return identity, nil
}

func prepareLocalState(path string) error {
	if filepath.Dir(path) != localStateRoot {
		return conflict("public ingress state path is outside its managed root")
	}
	for _, directory := range []string{localStateRoot, path} {
		if err := os.Mkdir(directory, 0o700); err != nil && !os.IsExist(err) {
			return errs.Wrap(errs.KindInternal, err)
		}
		var stat unix.Stat_t
		if err := unix.Lstat(directory, &stat); err != nil ||
			stat.Uid != 0 || stat.Mode&(unix.S_IFMT|0o777) != unix.S_IFDIR|0o700 {
			return conflict("public ingress state directory is not root-owned and private")
		}
	}
	return nil
}
