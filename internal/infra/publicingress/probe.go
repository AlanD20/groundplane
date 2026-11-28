package publicingress

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sync/errgroup"
)

const (
	probeWindow      = 90 * time.Second
	probeTimeout     = 3 * time.Second
	probeParallelism = 8
)

// RouteCheck is an independently captured, safe GET expectation. The
// optional response header can witness the identity of the expected backend.
type RouteCheck struct {
	RouteID     string
	Host        string
	RoutePath   string
	Path        string
	Status      int
	Header      string
	HeaderValue string
}

// ProbePlan binds the verified configuration receipt to its exact host plan
// and an independently selected safe request for every enabled route.
type ProbePlan struct {
	HostID    string
	Revision  int64
	Config    []byte
	Listeners []netip.AddrPort
	Checks    []RouteCheck
}

// NewRouteProbe qualifies the exact published addresses, public redirects, and
// TLS using system trust. It dials the Host directly with the captured HTTP
// hostname and TLS SNI. The lookup must supply predecessor and candidate
// evidence by receipt so Apply and recovery cannot qualify the wrong revision.
func NewRouteProbe(identity DockerIdentity, lookup func(context.Context, Receipt) (ProbePlan, error)) (Probe, error) {
	if !validDockerIdentity(identity) || lookup == nil {
		return nil, errs.New(errs.KindValidationFailed, "public ingress probe dependencies are invalid")
	}
	return func(ctx context.Context, receipt Receipt) error {
		if ctx == nil || receipt.RuntimeID != identity.RuntimeID {
			return conflict("public ingress probe has a different runtime identity")
		}
		plan, err := lookup(ctx, receipt)
		if err != nil {
			return err
		}
		targets, err := probeTargets(identity, receipt, plan)
		if err != nil {
			return err
		}
		return waitForReadiness(ctx, identity, targets)
	}, nil
}

func probeTargets(identity DockerIdentity, receipt Receipt, probe ProbePlan) ([]RouteCheck, error) {
	expected, err := Describe(
		Configuration{RuntimeID: identity.RuntimeID, Revision: probe.Revision, JSON: probe.Config},
	)
	if err != nil {
		return nil, err
	}
	if receipt != expected || probe.HostID != identity.HostID || !equalListeners(probe.Listeners, identity.Listeners) ||
		len(probe.Checks) > 512 {
		return nil, conflict("public ingress probe plan does not match the applied receipt")
	}
	checks := make(map[string]bool, len(probe.Checks))
	matches, err := configuredMatches(probe.Config)
	if err != nil {
		return nil, err
	}
	if len(matches) != len(probe.Checks) {
		return nil, conflict("public ingress probe does not cover the configured routes")
	}
	covered := make(map[readinessMatch]bool, len(matches))
	for _, check := range probe.Checks {
		if checks[check.RouteID] || !validRouteCheck(check) {
			return nil, conflict("public ingress route has duplicate or invalid readiness evidence")
		}
		want := readinessMatch{host: check.Host, path: check.RoutePath}
		if want.path == "/" {
			want.path = "/*"
		}
		if covered[want] || firstMatch(matches, check.Host, check.Path) != want {
			return nil, conflict("public ingress readiness path selects a different route")
		}
		covered[want] = true
		checks[check.RouteID] = true
	}
	return probe.Checks, nil
}

type readinessMatch struct{ host, path string }

// Pull the effective ordered matchers from the sealed renderer output, so a
// shorter, overlapping route cannot qualify a more specific route by accident.
func configuredMatches(raw []byte) ([]readinessMatch, error) {
	var config struct {
		Apps struct {
			HTTP struct {
				Servers map[string]struct {
					Routes []struct {
						Match []struct{ Host, Path []string } `json:"match"`
					} `json:"routes"`
				} `json:"servers"`
			} `json:"http"`
		} `json:"apps"`
	}
	if err := json.Unmarshal(raw, &config); err != nil {
		return nil, conflict("public ingress probe configuration is invalid")
	}
	secure := config.Apps.HTTP.Servers["public-https"].Routes
	if len(secure) == 0 || len(secure[len(secure)-1].Match) != 0 {
		return nil, conflict("public ingress probe configuration has no final fallback")
	}
	matches := make([]readinessMatch, 0, len(secure)-1)
	for _, route := range secure[:len(secure)-1] {
		if len(route.Match) != 1 || len(route.Match[0].Host) != 1 || len(route.Match[0].Path) != 1 {
			return nil, conflict("public ingress probe configuration has an unsupported route matcher")
		}
		matches = append(matches, readinessMatch{host: route.Match[0].Host[0], path: route.Match[0].Path[0]})
	}
	return matches, nil
}

func firstMatch(matches []readinessMatch, host, path string) readinessMatch {
	for _, match := range matches {
		if match.host == host && matchesPath(match.path, path) {
			return match
		}
	}
	return readinessMatch{}
}

func equalListeners(first, second []netip.AddrPort) bool {
	if len(first) != len(second) {
		return false
	}
	want := make(map[netip.AddrPort]bool, len(second))
	for _, endpoint := range second {
		want[endpoint] = true
	}
	for _, endpoint := range first {
		if !want[endpoint] {
			return false
		}
		delete(want, endpoint)
	}
	return len(want) == 0
}

func validRouteCheck(check RouteCheck) bool {
	path, err := url.ParseRequestURI(check.Path)
	if err != nil || !validHostname(check.Host) || ids.Validate(ids.KindRoute, check.RouteID) != nil ||
		len(check.Path) > 2048 || !strings.HasPrefix(check.Path, "/") ||
		strings.HasPrefix(check.Path, "//") || path.IsAbs() || path.RawQuery != "" || path.ForceQuery ||
		path.EscapedPath() != check.Path || strings.ContainsAny(check.Path, "%#") || check.Status < 200 || check.Status > 499 ||
		(check.Status == http.StatusNotFound && check.Header == "") ||
		(check.Header == "") != (check.HeaderValue == "") ||
		strings.ContainsAny(check.Header+check.HeaderValue, "\r\n") {
		return false
	}
	return check.RoutePath == "/" || matchesPath(check.RoutePath, check.Path)
}

func matchesPath(pattern, path string) bool {
	prefix, wildcard := strings.CutSuffix(pattern, "*")
	if wildcard {
		return strings.HasPrefix(prefix, "/") && strings.HasPrefix(strings.ToLower(path), strings.ToLower(prefix))
	}
	return strings.EqualFold(pattern, path)
}

func validHostname(name string) bool {
	if len(name) > 253 || !strings.Contains(name, ".") {
		return false
	}
	for label := range strings.SplitSeq(name, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
				return false
			}
		}
	}
	return true
}

func waitForReadiness(ctx context.Context, identity DockerIdentity, targets []RouteCheck) error {
	bounded, cancel := context.WithTimeout(ctx, probeWindow)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		last := checkReadiness(bounded, identity, targets)
		if last == nil {
			return nil
		}
		select {
		case <-bounded.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return last
		case <-ticker.C:
		}
	}
}

func checkReadiness(ctx context.Context, identity DockerIdentity, targets []RouteCheck) error {
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(probeParallelism)
	for _, endpoint := range identity.Listeners {
		group.Go(func() error { return checkListener(ctx, endpoint) })
	}
	// Docker ownership and the readiness listener checks cover every bind.
	// The same Caddy runtime applies the routes across those binds.
	address := identity.Listeners[0].Addr()
	for _, target := range targets {
		group.Go(func() error { return checkRoute(ctx, address, target) })
	}
	return group.Wait()
}

func checkListener(ctx context.Context, endpoint netip.AddrPort) error {
	if endpoint.Port() == 80 {
		client := directClient(endpoint)
		status, _, err := requestStatus(ctx, client, "http://groundplane-unbound.invalid/")
		if err != nil || status != http.StatusNotFound {
			return errs.New(errs.KindRequestFailed, "public ingress HTTP listener did not reject an unbound host")
		}
		return nil
	}
	dialer := net.Dialer{Timeout: probeTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", endpoint.String())
	if err != nil {
		return errs.New(errs.KindRequestFailed, "public ingress HTTPS listener is unreachable")
	}
	_ = conn.Close()
	return nil
}

func checkRoute(ctx context.Context, address netip.Addr, target RouteCheck) error {
	plain := directClient(netip.AddrPortFrom(address, 80))
	secure := directClient(netip.AddrPortFrom(address, 443))
	location := "https://" + target.Host + target.Path
	status, headers, err := requestStatus(ctx, plain, "http://"+target.Host+target.Path)
	if err != nil || status != http.StatusPermanentRedirect || headers.Get("Location") != location {
		return errs.Newf(
			errs.KindRequestFailed,
			"public ingress route %s did not serve its approved HTTPS redirect",
			target.RouteID,
		)
	}
	status, headers, err = requestStatus(ctx, secure, location)
	if err != nil || status != target.Status || target.Header != "" &&
		headers.Get(target.Header) != target.HeaderValue {
		return errs.Newf(
			errs.KindRequestFailed,
			"public ingress route %s did not serve its approved TLS response",
			target.RouteID,
		)
	}
	return nil
}

func directClient(endpoint netip.AddrPort) *http.Client {
	dialer := net.Dialer{Timeout: probeTimeout}
	return &http.Client{
		Timeout:       probeTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{
			DisableKeepAlives:      true,
			DisableCompression:     true,
			MaxResponseHeaderBytes: 64 << 10,
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, network, endpoint.String())
			},
		},
	}
}

func requestStatus(ctx context.Context, client *http.Client, location string) (int, http.Header, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, location, nil)
	if err != nil {
		return 0, nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, errs.Wrap(errs.KindRequestFailed, err)
	}
	if err := response.Body.Close(); err != nil {
		return 0, nil, errs.Wrap(errs.KindRequestFailed, fmt.Errorf("close readiness response: %w", err))
	}
	return response.StatusCode, response.Header, nil
}
