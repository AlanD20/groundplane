package dnsresolver

import (
	"net/netip"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestBuildRouteHostProjectionIncludesOnlyObservedRoutes(t *testing.T) {
	at := time.Unix(1_700_000_000, 0).UTC()
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	desiredRevisionID := ids.NewAt(ids.KindTask, at, 2)
	serviceID := ids.NewAt(ids.KindService, at, 3)
	inputs := []RouteHostInput{
		{EnvironmentID: environmentID, DesiredRevisionID: desiredRevisionID, AppliedRevision: 41,
			RouteID: ids.NewAt(ids.KindRoute, at, 4), ServiceID: serviceID, Host: "web.example.test",
			Address: netip.MustParseAddr("192.0.2.10"), State: RouteHostObserved},
		{EnvironmentID: environmentID, DesiredRevisionID: desiredRevisionID, AppliedRevision: 42,
			RouteID: ids.NewAt(ids.KindRoute, at, 5), ServiceID: serviceID, Host: "pending.example.test",
			Address: netip.MustParseAddr("192.0.2.10"), State: RouteHostPending},
	}
	projection, err := BuildRouteHostProjection(9, inputs)
	if err != nil || len(projection.Routes) != 1 || projection.Resolver.InputRevision != 9 {
		t.Fatalf("projection = %#v, error = %v", projection, err)
	}
	reversed := []RouteHostInput{inputs[1], inputs[0]}
	again, err := BuildRouteHostProjection(9, reversed)
	if err != nil || again.Resolver.InputSHA256 != projection.Resolver.InputSHA256 {
		t.Fatalf("reversed projection digest changed: %x, %v", again.Resolver.InputSHA256, err)
	}
}
