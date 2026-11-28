package config

import (
	"fmt"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/imageref"
)

// HTTPAddresses includes the internal endpoint without opening duplicate sockets.
func (c ControllerConfig) HTTPAddresses() []string {
	addresses := append([]string(nil), c.Listen.HTTP...)
	for _, address := range addresses {
		if address == c.Listen.InternalHTTP {
			return addresses
		}
	}
	return append(addresses, c.Listen.InternalHTTP)
}

// InternalAPI is independent of operator interface selection.
func (c ControllerConfig) InternalAPI() string { return c.Listen.InternalHTTP }

func (c ControllerConfig) validateGitHubWebhookListener() error {
	if c.Listen.GitHubWebhooks == "" {
		return nil
	}
	webhook, err := netip.ParseAddrPort(c.Listen.GitHubWebhooks)
	if err != nil || !webhook.Addr().Is4() || webhook.Port() == 0 ||
		!(webhook.Addr().IsLoopback() || webhook.Addr().IsPrivate()) {
		return fmt.Errorf(
			"config: controller listen.github_webhooks requires a private or loopback IPv4 address and port",
		)
	}
	for _, value := range append(c.HTTPAddresses(), c.Listen.ScopedRunnerHTTP, c.Listen.ScopedRunnerRegistry) {
		address, err := netip.ParseAddrPort(value)
		if err == nil && address.Port() == webhook.Port() &&
			(address.Addr() == webhook.Addr() || address.Addr().IsUnspecified()) {
			return fmt.Errorf("config: controller listen.github_webhooks conflicts with another HTTP listener")
		}
	}
	return nil
}

func (c ControllerConfig) validateScopedRunnerListeners(internal netip.AddrPort) error {
	if c.Listen.ScopedRunnerHTTP == "" && c.Listen.ScopedRunnerRegistry == "" {
		return nil
	}
	if !internal.Addr().IsPrivate() || c.Listen.ScopedRunnerHTTP == "" {
		return fmt.Errorf("config: scoped Runner access requires a private bridge and listen.scoped_runner_http")
	}
	if c.Listen.ScopedRunnerRegistry != "" && !imageref.IsDigestPinned(c.Runner.ScopedImage) {
		return fmt.Errorf("config: scoped Runner registry requires scoped_runner_image pinned by digest")
	}
	reserved := map[netip.AddrPort]bool{
		internal: true,
		netip.AddrPortFrom(internal.Addr(), 5000): true,
	}
	for _, value := range c.Listen.HTTP {
		if address, err := netip.ParseAddrPort(value); err == nil {
			reserved[address] = true
		}
	}
	for _, candidate := range []struct{ key, value string }{
		{"listen.scoped_runner_http", c.Listen.ScopedRunnerHTTP},
		{"listen.scoped_runner_registry", c.Listen.ScopedRunnerRegistry},
	} {
		if candidate.value == "" {
			continue
		}
		address, err := netip.ParseAddrPort(candidate.value)
		if err != nil || address.Addr() != internal.Addr() || address.Port() == 0 || reserved[address] {
			return fmt.Errorf(
				"config: controller %s must use an unreserved port on the private internal IPv4 address",
				candidate.key,
			)
		}
		reserved[address] = true
	}
	return nil
}
