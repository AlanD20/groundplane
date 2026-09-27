package runner

import (
	"context"
	"encoding/json"
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

func (operations *localOperations) EnsureEgress(ctx context.Context, plan corerunner.Plan) error {
	table := nftTable(plan)
	exists, err := operations.egressExists(ctx, plan)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	denied := make([]string, 0, len(plan.Egress.DeniedCIDRs))
	for _, prefix := range plan.Egress.DeniedCIDRs {
		denied = append(denied, prefix.String())
	}
	denied = append(denied, plan.Network.RunnerPool.String())
	source := fmt.Sprintf(
		"table inet %s {\n comment \"%s\"\n"+
			" chain outbound { ip daddr %s tcp dport { %d, 53, 5000 } accept; ip daddr %s udp dport 53 accept; ip daddr { %s } reject; }\n"+
			" chain output { type filter hook output priority 0; policy accept; meta skuid %d jump outbound; }\n"+
			" chain input { type filter hook input priority 0; policy accept; iifname \"%s\" jump outbound; }\n"+
			" chain forward { type filter hook forward priority 0; policy accept; iifname \"%s\" jump outbound; }\n}\n",
		table,
		plan.IdentityDigest(),
		plan.Egress.ControllerEndpoint.Addr(),
		plan.Egress.ControllerEndpoint.Port(),
		plan.Egress.ControllerEndpoint.Addr(),
		strings.Join(denied, ", "),
		plan.Egress.SourceUID,
		plan.Network.BridgeName,
		plan.Network.BridgeName,
	)
	_, err = operations.runInput(ctx, []byte(source), "nft", "-f", "-")
	return err
}

func (operations *localOperations) ObserveEgress(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	exists, err := operations.egressExists(ctx, plan)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	if !exists {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner egress policy is absent")
	}
	return applied(corerunner.StepEnsureEgress), nil
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

func (operations *localOperations) RemoveEgress(ctx context.Context, plan corerunner.Plan) (string, error) {
	exists, err := operations.egressExists(ctx, plan)
	if err != nil {
		return "", err
	}
	if exists {
		if _, err := operations.run(ctx, "nft", "delete", "table", "inet", nftTable(plan)); err != nil {
			return "", err
		}
	}
	return receipt(plan, corerunner.StepRemoveEgress), nil
}

func (operations *localOperations) ObserveEgressAbsent(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	exists, err := operations.egressExists(ctx, plan)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	if exists {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner egress policy still exists")
	}
	return absent(plan, corerunner.StepRemoveEgress), nil
}

type nftInventory struct {
	Entries []struct {
		Table *struct {
			Family  string `json:"family"`
			Name    string `json:"name"`
			Comment string `json:"comment"`
		} `json:"table"`
	} `json:"nftables"`
}

func (operations *localOperations) egressExists(ctx context.Context, plan corerunner.Plan) (bool, error) {
	result, err := operations.run(ctx, "nft", "--json", "list", "tables")
	if err != nil {
		return false, err
	}
	var inventory nftInventory
	if err := json.Unmarshal(result.Stdout, &inventory); err != nil || inventory.Entries == nil {
		return false, errs.New(errs.KindStateConflict, "Runner firewall inventory is unavailable")
	}
	found := false
	for _, entry := range inventory.Entries {
		if entry.Table != nil && entry.Table.Family == "inet" && entry.Table.Name == nftTable(plan) {
			found = true
		}
	}
	if !found {
		return false, nil
	}
	result, err = operations.run(ctx, "nft", "--json", "list", "table", "inet", nftTable(plan))
	if err != nil {
		return false, err
	}
	var owned nftInventory
	if err := json.Unmarshal(result.Stdout, &owned); err != nil {
		return false, errs.New(errs.KindStateConflict, "Runner firewall identity is unavailable")
	}
	for _, entry := range owned.Entries {
		if entry.Table != nil && entry.Table.Family == "inet" && entry.Table.Name == nftTable(plan) &&
			entry.Table.Comment == plan.IdentityDigest() {
			return true, nil
		}
	}
	return false, errs.New(errs.KindStateConflict, "Runner firewall belongs to another allocation")
}

func ensureNetwork(ctx context.Context, engine *client.Client, plan corerunner.Plan) error {
	err := observeNetwork(ctx, engine, plan)
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
