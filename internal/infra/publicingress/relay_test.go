package publicingress

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
)

func TestRelayChecksTheCapturedOriginAndPath(t *testing.T) {
	origin := netip.MustParseAddrPort("10.90.1.11:8080")
	proof := strings.Repeat("a", 64)
	routes := []routeOrigin{{host: "app.example.org", path: "/app/*", dial: origin, proof: proof}}
	for _, check := range []struct {
		name, url, address, proof string
		allowed                   bool
	}{
		{"approved", "https://app.example.org/app/live", origin.String(), proof, true},
		{"path", "https://app.example.org/internal", origin.String(), proof, false},
		{"host", "https://admin.example.org/app/live", origin.String(), proof, false},
		{"private port", "https://app.example.org/app/live", "10.90.1.11:2019", proof, false},
		{"proof", "https://app.example.org/app/live", origin.String(), strings.Repeat("b", 64), false},
		{"encoded path", "https://app.example.org/app/%2e%2e/internal", origin.String(), proof, false},
	} {
		t.Run(check.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, check.url, nil)
			// The relay receives an origin-form request from the owning Caddy.
			request.URL.Scheme, request.URL.Host = "", ""
			request.Header.Set(ingressexecution.RelayOriginHeader, check.address)
			request.Header.Set(ingressexecution.RelayProofHeader, check.proof)
			got, allowed := relayTarget(request, routes)
			if allowed != check.allowed || allowed && got != origin {
				t.Fatalf("relay target = %v, allowed = %v", got, allowed)
			}
		})
	}
}

func TestRelayRejectsConnectionsFromOutsideTheEdge(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/app", nil)
	request.RemoteAddr = "10.91.0.3:1234"
	response := httptest.NewRecorder()
	relay := newRelay(nil, netip.MustParseAddr("10.91.0.2"))
	relay.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("untrusted peer status = %d", response.Code)
	}
}
