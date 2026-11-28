package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"path/filepath"
	"slices"
	"strings"

	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type egressState uint8

const (
	egressAbsent egressState = iota
	egressLegacy
	egressRegistration
	egressScoped
)

func (operations *localOperations) EnsureEgress(ctx context.Context, plan corerunner.Plan) error {
	state, err := operations.egressState(ctx, plan)
	if err != nil {
		return err
	}
	want := expectedEgress(plan)
	if state == want {
		return nil
	}
	source := legacyFirewall(plan)
	switch want {
	case egressRegistration:
		if state != egressAbsent {
			return errs.New(errs.KindStateConflict, "Runner registration firewall has unexpected ownership")
		}
		source = scopedFirewall(plan)
	case egressScoped:
		if state == egressLegacy || state == egressRegistration {
			if err := requireScopedCredential(plan); err != nil {
				return err
			}
			path := filepath.Join(runnerDockerConfig(plan), "config.json")
			if err := verifyScopedDockerConfig(path, plan.Identity.UID); err != nil {
				return err
			}
		}
		source = scopedFirewall(plan)
		if state == egressLegacy || state == egressRegistration {
			// nft applies the whole batch atomically. A failed cutover retains
			// the preceding table and leaves no unfiltered intermediate state.
			source = "delete table inet " + nftTable(plan) + "\n" + source
		}
	case egressLegacy:
		if state != egressAbsent {
			return errs.New(errs.KindStateConflict, "Legacy Runner cannot replace a restricted firewall")
		}
	}
	if _, err := operations.runInput(ctx, []byte(source), "nft", "-f", "-"); err != nil {
		return err
	}
	_, err = operations.ObserveEgress(ctx, plan)
	return err
}

func (operations *localOperations) ObserveEgress(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	state, err := operations.egressState(ctx, plan)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	if state != expectedEgress(plan) {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner egress policy is not applied")
	}
	return applied(corerunner.StepEnsureEgress), nil
}

func (operations *localOperations) RemoveEgress(ctx context.Context, plan corerunner.Plan) (string, error) {
	state, err := operations.egressState(ctx, plan)
	if err != nil {
		return "", err
	}
	if state != egressAbsent && state != expectedEgress(plan) {
		return "", errs.New(errs.KindStateConflict, "Runner egress cutover has not replaced its predecessor")
	}
	if state != egressAbsent {
		if _, err := operations.run(ctx, "nft", "delete", "table", "inet", nftTable(plan)); err != nil {
			return "", err
		}
	}
	return receipt(plan, corerunner.StepRemoveEgress), nil
}

func expectedEgress(plan corerunner.Plan) egressState {
	if plan.Egress.RegistrationOnly {
		return egressRegistration
	}
	if plan.Egress.ScopedEndpoint.IsValid() {
		return egressScoped
	}
	return egressLegacy
}

func (operations *localOperations) ObserveEgressAbsent(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	state, err := operations.egressState(ctx, plan)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	if state != egressAbsent {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner egress policy still exists")
	}
	return absent(plan, corerunner.StepRemoveEgress), nil
}

func (operations *localOperations) egressState(ctx context.Context, plan corerunner.Plan) (egressState, error) {
	result, err := operations.run(ctx, "nft", "--json", "list", "tables")
	if err != nil {
		return egressAbsent, err
	}
	var tables nftInventory
	if err := json.Unmarshal(result.Stdout, &tables); err != nil || tables.Entries == nil {
		return egressAbsent, errs.New(errs.KindStateConflict, "Runner firewall inventory is unavailable")
	}
	if !slices.ContainsFunc(tables.Entries, func(entry nftEntry) bool {
		return entry.Table != nil && entry.Table.Family == "inet" && entry.Table.Name == nftTable(plan)
	}) {
		return egressAbsent, nil
	}
	result, err = operations.run(ctx, "nft", "--json", "--numeric", "list", "table", "inet", nftTable(plan))
	if err != nil {
		return egressAbsent, err
	}
	var inventory nftInventory
	if err := json.Unmarshal(result.Stdout, &inventory); err != nil || inventory.Entries == nil {
		return egressAbsent, errs.New(errs.KindStateConflict, "Runner firewall identity is unavailable")
	}
	for _, entry := range inventory.Entries {
		if entry.Table == nil || entry.Table.Family != "inet" || entry.Table.Name != nftTable(plan) {
			continue
		}
		if entry.Table.Comment == plan.IdentityDigest() {
			return egressLegacy, nil
		}
		if plan.Egress.ScopedEndpoint.IsValid() && entry.Table.Comment == scopedFirewallComment(plan) &&
			matchesScopedFirewall(inventory, plan) {
			return egressScoped, nil
		}
		if plan.Egress.RegistrationOnly || plan.Egress.ScopedEndpoint.IsValid() {
			registration := plan
			registration.Egress.RegistrationOnly = true
			registration.Egress.ScopedEndpoint, registration.Egress.ScopedRegistry = netip.AddrPort{}, netip.AddrPort{}
			if entry.Table.Comment == scopedFirewallComment(registration) &&
				matchesScopedFirewall(inventory, registration) {
				return egressRegistration, nil
			}
		}
	}
	return egressAbsent, errs.New(errs.KindStateConflict, "Runner firewall policy differs from its captured plan")
}

func legacyFirewall(plan corerunner.Plan) string {
	denied := make([]string, 0, len(plan.Egress.DeniedCIDRs)+1)
	for _, prefix := range plan.Egress.DeniedCIDRs {
		denied = append(denied, prefix.String())
	}
	denied = append(denied, plan.Network.RunnerPool.String())
	return fmt.Sprintf(
		"table inet %s {\n comment %q\n"+
			" chain outbound { ip daddr %s tcp dport { %d, 53, 5000 } accept; ip daddr %s udp dport 53 accept; ip daddr { %s } drop; }\n"+
			" chain output { type filter hook output priority 0; policy accept; meta skuid %d jump outbound; }\n"+
			" chain input { type filter hook input priority 0; policy accept; iifname %q jump outbound; }\n"+
			" chain forward { type filter hook forward priority 0; policy accept; iifname %q jump outbound; }\n}\n",
		nftTable(
			plan,
		),
		plan.IdentityDigest(),
		plan.Egress.ControllerEndpoint.Addr(),
		plan.Egress.ControllerEndpoint.Port(),
		plan.Egress.ControllerEndpoint.Addr(),
		strings.Join(denied, ", "),
		plan.Egress.SourceUID,
		plan.Network.BridgeName,
		plan.Network.BridgeName,
	)
}

func scopedFirewallComment(plan corerunner.Plan) string {
	if plan.Egress.RegistrationOnly {
		return "registration-v1:" + plan.IdentityDigest()
	}
	plan.RuntimeEpoch = 0
	return "scoped-v1:" + plan.Digest()
}

type firewallChain struct {
	name  string
	hook  string
	rules []string
}

// scopedFirewallChains is shared by the kernel rule writer and its observation
// so a flushed or changed table cannot satisfy scoped readiness by name alone.
func scopedFirewallChains(plan corerunner.Plan) []firewallChain {
	dns := plan.Egress.ControllerEndpoint.Addr()
	rules := []string{
		fmt.Sprintf("ip daddr %s tcp dport 53 accept", dns),
		fmt.Sprintf("ip daddr %s udp dport 53 accept", dns),
	}
	if plan.Egress.ScopedEndpoint.IsValid() {
		rules = append(rules, allowScopedEndpoint(plan.Egress.ScopedEndpoint))
	}
	if plan.Egress.ScopedRegistry.IsValid() {
		rules = append(rules, allowScopedEndpoint(plan.Egress.ScopedRegistry))
	}
	// Match both traffic emitted on the host by rootless networking and
	// packets on the Runner bridge. Refuse alternate local listeners even if
	// their addresses are not advertised by the deployment.
	rules = append(rules, "meta nfproto ipv6 drop", "fib daddr type local drop")
	for _, prefix := range scopedDeniedNetworks(plan) {
		rules = append(rules, "ip daddr "+addressOrPrefix(prefix)+" drop")
	}
	if plan.Egress.RegistrationOnly {
		rules = append(rules, "tcp dport 443 accept", "meta nfproto ipv4 drop")
	}
	return []firewallChain{
		{name: "outbound", rules: rules},
		{
			name:  "output",
			hook:  "output",
			rules: []string{fmt.Sprintf("meta skuid %d jump outbound", plan.Egress.SourceUID)},
		},
		{name: "input", hook: "input", rules: []string{"meta iifname " + plan.Network.BridgeName + " jump outbound"}},
		{
			name:  "forward",
			hook:  "forward",
			rules: []string{"meta iifname " + plan.Network.BridgeName + " jump outbound"},
		},
	}
}

func scopedDeniedNetworks(plan corerunner.Plan) []netip.Prefix {
	prefixes := append(slices.Clone(plan.Egress.DeniedCIDRs), plan.Network.RunnerPool)
	for _, network := range [...]string{"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8",
		"169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16", "224.0.0.0/3"} {
		prefixes = append(prefixes, netip.MustParsePrefix(network))
	}
	slices.SortFunc(prefixes, func(left, right netip.Prefix) int {
		if order := left.Addr().Compare(right.Addr()); order != 0 {
			return order
		}
		return left.Bits() - right.Bits()
	})
	return slices.Compact(prefixes)
}

func allowScopedEndpoint(endpoint netip.AddrPort) string {
	return fmt.Sprintf("ip daddr %s tcp dport %d accept", endpoint.Addr(), endpoint.Port())
}

func addressOrPrefix(prefix netip.Prefix) string {
	if prefix.IsSingleIP() {
		return prefix.Addr().String()
	}
	return prefix.String()
}

func scopedFirewall(plan corerunner.Plan) string {
	var source strings.Builder
	fmt.Fprintf(&source, "table inet %s {\n comment %q\n", nftTable(plan), scopedFirewallComment(plan))
	for _, chain := range scopedFirewallChains(plan) {
		fmt.Fprintf(&source, " chain %s {\n", chain.name)
		if chain.hook != "" {
			fmt.Fprintf(&source, "  type filter hook %s priority 0; policy accept;\n", chain.hook)
		}
		for _, rule := range chain.rules {
			fmt.Fprintf(&source, "  %s;\n", rule)
		}
		source.WriteString(" }\n")
	}
	source.WriteString("}\n")
	return source.String()
}
