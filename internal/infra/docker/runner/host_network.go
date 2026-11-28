package runner

import (
	"context"
	"strconv"

	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

func (operations *localOperations) EnsureNetwork(ctx context.Context, plan corerunner.Plan) error {
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return err
	}
	defer engine.Close()
	return ensureNetwork(ctx, engine, plan)
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
	if err := observeNetwork(ctx, engine, plan); err != nil {
		return corerunner.StepEvidence{}, err
	}
	return applied(corerunner.StepEnsureNetwork), nil
}

func (operations *localOperations) RemoveNetwork(ctx context.Context, plan corerunner.Plan) (string, error) {
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return "", err
	}
	defer engine.Close()
	result, err := engine.NetworkInspect(ctx, plan.Network.Name, client.NetworkInspectOptions{})
	if containerderrdefs.IsNotFound(err) {
		return receipt(plan, corerunner.StepRemoveNetwork), nil
	}
	if err != nil {
		return "", dockerError(ctx, "inspect Runner network before removal", err)
	}
	if err := validateNetwork(result.Network, plan); err != nil {
		return "", err
	}
	_, err = engine.NetworkRemove(ctx, result.Network.ID, client.NetworkRemoveOptions{})
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

func ensureNetwork(ctx context.Context, engine *client.Client, plan corerunner.Plan) error {
	err := observeNetwork(ctx, engine, plan)
	if err == nil {
		return nil
	}
	if !containerderrdefs.IsNotFound(err) {
		return err
	}
	enableIPv4 := true
	options := client.NetworkCreateOptions{
		Driver: "bridge", EnableIPv4: &enableIPv4,
		IPAM: &network.IPAM{Driver: "default", Config: []network.IPAMConfig{{
			Subnet: plan.Network.Subnet, Gateway: plan.Network.Gateway,
		}}},
		Labels: map[string]string{
			"com.groundplane.managed":   "true",
			"com.groundplane.kind":      "runner-network",
			"groundplane.runner.id":     plan.RunnerID,
			"groundplane.runtime.epoch": strconv.FormatUint(plan.RuntimeEpoch, 10),
		},
	}
	options.Options = map[string]string{"com.docker.network.bridge.name": plan.Network.BridgeName}
	if _, err := engine.NetworkCreate(ctx, plan.Network.Name, options); err != nil {
		return dockerError(ctx, "create Runner network", err)
	}
	return observeNetwork(ctx, engine, plan)
}

func observeNetwork(ctx context.Context, engine *client.Client, plan corerunner.Plan) error {
	result, err := engine.NetworkInspect(ctx, plan.Network.Name, client.NetworkInspectOptions{})
	if err != nil {
		return dockerError(ctx, "inspect Runner network", err)
	}
	return validateNetwork(result.Network, plan)
}

func validateNetwork(inspected network.Inspect, plan corerunner.Plan) error {
	if inspected.ID == "" || inspected.Name != plan.Network.Name || inspected.Driver != "bridge" ||
		inspected.IPAM.Config == nil ||
		inspected.Labels["groundplane.runner.id"] != plan.RunnerID ||
		inspected.Labels["groundplane.runtime.epoch"] != strconv.FormatUint(plan.RuntimeEpoch, 10) ||
		len(inspected.IPAM.Config) != 1 ||
		inspected.IPAM.Config[0].Subnet != plan.Network.Subnet ||
		inspected.IPAM.Config[0].Gateway != plan.Network.Gateway {
		return errs.New(errs.KindStateConflict, "Runner network identity changed")
	}
	if inspected.Options["com.docker.network.bridge.name"] != plan.Network.BridgeName {
		return errs.New(errs.KindStateConflict, "Runner bridge identity changed")
	}
	return nil
}
