package runner

import (
	"context"
	"fmt"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"strconv"
	"strings"
)

func (operations *localOperations) EnsureNetwork(ctx context.Context, plan corerunner.Plan) error {
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return err
	}
	defer engine.Close()
	return ensureNetwork(ctx, engine, plan, true)
}

func (operations *localOperations) ObserveNetwork(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	defer engine.Close()
	if err := observeNetwork(ctx, engine, plan, true); err != nil {
		return corerunner.StepEvidence{}, err
	}
	return applied(corerunner.StepEnsureNetwork), nil
}

func (operations *localOperations) EnsureEgress(ctx context.Context, plan corerunner.Plan) error {
	table := nftTable(plan)
	_, _ = operations.run(ctx, "nft", "delete", "table", "inet", table)
	denied := make([]string, 0, len(plan.Egress.DeniedCIDRs))
	for _, prefix := range plan.Egress.DeniedCIDRs {
		denied = append(denied, prefix.String())
	}
	source := fmt.Sprintf(
		"table inet %s { chain output { type filter hook output priority 0; policy accept; ip daddr %s accept; meta skuid %d ip daddr { %s } reject; } }\n",
		table,
		plan.Egress.ControllerEndpoint.Addr(),
		plan.Egress.SourceUID,
		strings.Join(denied, ", "),
	)
	_, err := operations.runInput(ctx, []byte(source), "nft", "-f", "-")
	return err
}

func (operations *localOperations) ObserveEgress(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	result, err := operations.run(ctx, "nft", "list", "table", "inet", nftTable(plan))
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	want := strconv.FormatUint(uint64(plan.Egress.SourceUID), 10)
	if !strings.Contains(string(result.Stdout), want) {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "runner egress policy identity changed")
	}
	return applied(corerunner.StepEnsureEgress), nil
}

func (operations *localOperations) RemoveNetwork(ctx context.Context, plan corerunner.Plan) (string, error) {
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return "", err
	}
	defer engine.Close()
	_, err = engine.NetworkRemove(ctx, plan.Network.Name, client.NetworkRemoveOptions{})
	if err != nil && !containerderrdefs.IsNotFound(err) {
		return "", dockerError(ctx, "remove Runner network", err)
	}
	return receipt(plan, corerunner.StepRemoveNetwork), nil
}

func (operations *localOperations) ObserveNetworkAbsent(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	defer engine.Close()
	_, err = engine.NetworkInspect(ctx, plan.Network.Name, client.NetworkInspectOptions{})
	if err == nil || !containerderrdefs.IsNotFound(err) {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner network still exists")
	}
	return absent(plan, corerunner.StepRemoveNetwork), nil
}

func (operations *localOperations) RemoveEgress(ctx context.Context, plan corerunner.Plan) (string, error) {
	result, err := operations.run(ctx, "nft", "delete", "table", "inet", nftTable(plan))
	if err != nil && result.ExitCode != 1 {
		return "", err
	}
	return receipt(plan, corerunner.StepRemoveEgress), nil
}

func (operations *localOperations) ObserveEgressAbsent(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	result, err := operations.run(ctx, "nft", "list", "table", "inet", nftTable(plan))
	if err == nil || result.ExitCode == 0 {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner egress policy still exists")
	}
	return absent(plan, corerunner.StepRemoveEgress), nil
}

func ensureNetwork(ctx context.Context, engine *client.Client, plan corerunner.Plan, rootful bool) error {
	err := observeNetwork(ctx, engine, plan, rootful)
	if err == nil {
		return nil
	}
	if !containerderrdefs.IsNotFound(rootCause(err)) {
		return err
	}
	enableIPv4 := true
	options := client.NetworkCreateOptions{
		Driver: "bridge", EnableIPv4: &enableIPv4,
		IPAM: &network.IPAM{Driver: "default", Config: []network.IPAMConfig{{
			Subnet: plan.Network.Subnet, Gateway: plan.Network.Gateway,
		}}},
		Labels: map[string]string{
			"groundplane.runner.id":     plan.RunnerID,
			"groundplane.runtime.epoch": strconv.FormatUint(plan.RuntimeEpoch, 10),
		},
	}
	if rootful {
		options.Options = map[string]string{"com.docker.network.bridge.name": plan.Network.BridgeName}
	}
	if _, err := engine.NetworkCreate(ctx, plan.Network.Name, options); err != nil {
		return dockerError(ctx, "create Runner network", err)
	}
	return observeNetwork(ctx, engine, plan, rootful)
}

func observeNetwork(ctx context.Context, engine *client.Client, plan corerunner.Plan, rootful bool) error {
	result, err := engine.NetworkInspect(ctx, plan.Network.Name, client.NetworkInspectOptions{})
	if err != nil {
		return dockerError(ctx, "inspect Runner network", err)
	}
	inspected := result.Network
	if inspected.Name != plan.Network.Name || inspected.Driver != "bridge" || inspected.IPAM.Config == nil ||
		len(inspected.IPAM.Config) != 1 || inspected.IPAM.Config[0].Subnet != plan.Network.Subnet ||
		inspected.IPAM.Config[0].Gateway != plan.Network.Gateway {
		return errs.New(errs.KindStateConflict, "Runner network identity changed")
	}
	if rootful && inspected.Options["com.docker.network.bridge.name"] != plan.Network.BridgeName {
		return errs.New(errs.KindStateConflict, "Runner bridge identity changed")
	}
	return nil
}
