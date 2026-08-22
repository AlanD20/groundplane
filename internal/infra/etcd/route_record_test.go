package etcd

import (
	"bytes"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestRouteRecordPreservesTypedMatchAndTarget(t *testing.T) {
	// Rationale: an exposure edit must not collapse the separately typed host
	// and path or retarget the Route to a different Service.
	t.Parallel()
	record := routeRecordTestRecord(t, 1001)
	desired := record.Desired
	desired.Exposure = "internal"
	replacement, err := ReplaceRouteDesired(record, desired)
	if err != nil {
		t.Fatalf("ReplaceRouteDesired() error = %v", err)
	}
	if replacement.EnvironmentID != record.EnvironmentID || replacement.Desired.Host != record.Desired.Host ||
		replacement.Desired.Path != record.Desired.Path ||
		replacement.Desired.TargetServiceID != record.Desired.TargetServiceID ||
		replacement.Desired.TargetPort != record.Desired.TargetPort {
		t.Fatalf("replacement = %#v, want immutable fields from %#v", replacement, record)
	}
	desired.Path = "/changed/*"
	if _, err := ReplaceRouteDesired(record, desired); err == nil {
		t.Fatal("ReplaceRouteDesired() accepted a match change")
	}
}

func TestRouteRecordEnvelopeRoundTripsStrictly(t *testing.T) {
	// Rationale: malformed Route records must fail closed before they can feed
	// an ingress renderer.
	t.Parallel()
	record := routeRecordTestRecord(t, 1002)
	encoded, err := encodeRouteRecord(record)
	if err != nil {
		t.Fatalf("encodeRouteRecord() error = %v", err)
	}
	decoded, err := decodeRouteRecord(encoded)
	if err != nil || decoded != record {
		t.Fatalf("decodeRouteRecord() = %#v, %v, want %#v", decoded, err, record)
	}
	for name, corrupt := range map[string][]byte{
		"unknown":   bytes.Replace(encoded, []byte(`"desired":`), []byte(`"unknown":0,"desired":`), 1),
		"duplicate": bytes.Replace(encoded, []byte(`"desired":`), []byte(`"environment_id":"x","desired":`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeRouteRecord(corrupt); err == nil {
				t.Fatal("decodeRouteRecord() accepted corrupt record")
			}
		})
	}
}

func TestRouteRecordKeysUseStableEnvironmentScope(t *testing.T) {
	// Rationale: Route membership must key only by stable Environment and Route
	// ids, never mutable presentation paths.
	t.Parallel()
	record := routeRecordTestRecord(t, 1003)
	if got := routeKey(record.Desired.ID); got != "/v1/records/routes/"+record.Desired.ID {
		t.Fatalf("routeKey() = %q", got)
	}
	if got := routeOwnerKey(record.EnvironmentID, record.Desired.ID); got !=
		"/v1/indexes/routes/by-owner/environment/"+record.EnvironmentID+"/"+record.Desired.ID {
		t.Fatalf("routeOwnerKey() = %q", got)
	}
}

func routeRecordTestRecord(t *testing.T, offset int64) RouteRecord {
	t.Helper()
	record, err := NewRouteRecord(
		ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 1000),
		core.Route{
			ID: ids.NewAt(ids.KindRoute, serviceRecordTestTime(), offset), Host: "app.example.com",
			Path: "/api/*", TargetServiceID: ids.NewAt(ids.KindService, serviceRecordTestTime(), 1004),
			TargetPort: 8080, Exposure: "public",
		},
	)
	if err != nil {
		t.Fatalf("NewRouteRecord() error = %v", err)
	}
	return record
}
