package publicingress

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
	"golang.org/x/sync/errgroup"
)

func edgeName(hostID string) string { return "gp-public-ingress-" + strings.ToLower(hostID) }

func (local *Local) store(identity DockerIdentity) (*Store, error) {
	owner, err := NewDockerOwner(local.runner, identity, func(ctx context.Context, runtimeID string) error {
		return settlePriorReload(ctx, identity.StateDirectory, runtimeID)
	})
	if err != nil {
		return nil, err
	}
	var runtime *Caddy
	probe := func(ctx context.Context, wanted Receipt) error {
		raw, err := runtime.Active(ctx, wanted.RuntimeID)
		if err != nil {
			return err
		}
		active, err := Describe(Configuration{RuntimeID: wanted.RuntimeID, Revision: wanted.Revision, JSON: raw})
		if err != nil || active != wanted {
			return conflict("public ingress readiness observed a different edge revision")
		}
		return local.checkPrivateTransportAndTLS(ctx, identity, raw)
	}
	runtime, err = NewCaddy(local.runner, owner, probe)
	if err != nil {
		return nil, err
	}
	return New(identity.StateDirectory, &guardedReload{Caddy: runtime, state: identity.StateDirectory})
}

type routeOrigin struct {
	host  string
	path  string
	dial  netip.AddrPort
	relay netip.AddrPort
	proof string
}

func routeOrigins(raw []byte) ([]routeOrigin, error) {
	var cfg struct {
		Apps struct {
			HTTP struct {
				Servers map[string]struct {
					Routes []struct {
						Match  []struct{ Host, Path []string } `json:"match"`
						Handle []struct {
							Kind       string                  `json:"handler"`
							StatusCode int                     `json:"status_code"`
							Upstreams  []struct{ Dial string } `json:"upstreams"`
							Headers    struct {
								Request struct{ Set map[string][]string } `json:"request"`
							} `json:"headers"`
						} `json:"handle"`
					} `json:"routes"`
				} `json:"servers"`
			} `json:"http"`
		} `json:"apps"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, conflict("public ingress has invalid route origins")
	}
	routes := cfg.Apps.HTTP.Servers["public-https"].Routes
	if len(routes) == 0 || len(routes) > 513 {
		return nil, conflict("public ingress origin count is invalid")
	}
	origins := make([]routeOrigin, 0, len(routes)-1)
	for index, route := range routes {
		if len(route.Handle) != 1 {
			return nil, conflict("public ingress route has an invalid handler")
		}
		handler := route.Handle[0]
		if index == len(routes)-1 {
			if len(route.Match) != 0 || handler.Kind != "static_response" || handler.StatusCode != 404 {
				return nil, conflict("public ingress has no private fallback")
			}
			break
		}
		if len(route.Match) != 1 || len(route.Match[0].Host) != 1 || len(route.Match[0].Path) != 1 ||
			!validHostname(route.Match[0].Host[0]) ||
			handler.Kind != "reverse_proxy" || len(handler.Upstreams) != 1 {
			return nil, conflict("public ingress has an unsupported origin handler")
		}
		origin, err := netip.ParseAddrPort(handler.Upstreams[0].Dial)
		if err != nil || !validPrivateOrigin(origin) {
			return nil, conflict("public ingress origin is outside the approved private transport")
		}
		match := route.Match[0]
		captured := routeOrigin{host: match.Host[0], path: match.Path[0], dial: origin}
		values := handler.Headers.Request.Set
		if len(values) != 0 {
			address, proof := values[ingressexecution.RelayOriginHeader], values[ingressexecution.RelayProofHeader]
			if len(values) != 2 || len(address) != 1 || len(proof) != 1 ||
				origin.Port() != ingressexecution.RelayPort || !validRuntimeID(proof[0]) {
				return nil, conflict("public ingress has invalid relay authority")
			}
			private, err := netip.ParseAddrPort(address[0])
			if err != nil || !validPrivateOrigin(private) || private == origin {
				return nil, conflict("public ingress has an invalid relay origin")
			}
			captured.relay, captured.dial, captured.proof = origin, private, proof[0]
		}
		origins = append(origins, captured)
	}
	return origins, nil
}

func validPrivateOrigin(origin netip.AddrPort) bool {
	return origin.Addr().Is4() && origin.Addr().IsPrivate() && origin.Port() != 0
}

func (local *Local) checkPrivateTransportAndTLS(ctx context.Context, identity DockerIdentity, raw []byte) error {
	origins, err := routeOrigins(raw)
	if err != nil {
		return err
	}
	if err := local.checkOriginTransport(ctx, identity, origins); err != nil {
		return err
	}
	hosts := make([]string, 0, len(origins))
	for _, origin := range origins {
		hosts = append(hosts, origin.host)
	}
	slices.Sort(hosts)
	hosts = slices.Compact(hosts)
	bounded, cancel := context.WithTimeout(ctx, probeWindow)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		last := checkTLSOnce(bounded, identity.Listeners, hosts)
		if last == nil {
			return nil
		}
		select {
		case <-bounded.Done():
			return last
		case <-ticker.C:
		}
	}
}

func checkTLSOnce(ctx context.Context, listeners []netip.AddrPort, hosts []string) error {
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(probeParallelism)
	families := make(map[bool]bool, 2)
	for _, endpoint := range listeners {
		group.Go(func() error { return checkListener(ctx, endpoint) })
		if endpoint.Port() != 443 || families[endpoint.Addr().Is4()] {
			continue
		}
		families[endpoint.Addr().Is4()] = true
		for _, hostname := range hosts {
			group.Go(func() error {
				dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: probeTimeout},
					Config: &tls.Config{ServerName: hostname, MinVersion: tls.VersionTLS12}}
				bounded, cancel := context.WithTimeout(ctx, probeTimeout)
				defer cancel()
				connection, err := dialer.DialContext(bounded, "tcp", endpoint.String())
				if err != nil {
					return errs.Newf(
						errs.KindRequestFailed,
						"public ingress does not serve trusted TLS for %s",
						hostname,
					)
				}
				return connection.Close()
			})
		}
	}
	return group.Wait()
}

func (local *Local) checkOriginTransport(ctx context.Context, identity DockerIdentity, origins []routeOrigin) error {
	engine, err := client.New(client.WithHost(hostDockerSocket))
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer engine.Close()
	bridge, err := inspectBridge(ctx, engine, identity.HostID, identity.NetworkID)
	if err != nil {
		return err
	}
	if err := local.verifyRelayBridge(ctx, engine, bridge); err != nil {
		return err
	}
	if len(bridge.Containers) != 1 || bridge.Containers[identity.RuntimeID].IPv4Address.Addr() != identity.EdgeAddress {
		return conflict("public ingress bridge membership changed")
	}
	if _, err := local.inspectRelay(ctx, engine, identity.StateDirectory, true); err != nil {
		return err
	}
	relay := local.config.Relay.Listen()
	if err := probeTCP(ctx, relay); err != nil {
		return err
	}
	return checkRelayOrigins(ctx, relay, origins)
}

func checkRelayOrigins(ctx context.Context, relay netip.AddrPort, origins []routeOrigin) error {
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(probeParallelism)
	seen := make(map[netip.AddrPort]bool, len(origins))
	for _, origin := range origins {
		if origin.relay != relay || origin.proof == "" {
			return conflict("public ingress configuration has a different private relay binding")
		}
		if seen[origin.dial] {
			continue
		}
		seen[origin.dial] = true
		group.Go(func() error { return probeTCP(ctx, origin.dial) })
	}
	return group.Wait()
}
