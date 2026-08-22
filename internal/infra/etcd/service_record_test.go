package etcd

import (
	"bytes"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestNewServiceRecordDefaultsRuntimeIntentToRunning(t *testing.T) {
	// Rationale: direct create and Blueprint reconciliation must agree on the
	// accepted initial Controller-owned runtime state.
	record, err := NewServiceRecord(
		ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 1),
		serviceRecordTestDesired(),
		"",
	)
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	if record.Runtime.ServiceID != record.Desired.ID ||
		record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		t.Fatalf("runtime = %#v, desired id = %q", record.Runtime, record.Desired.ID)
	}
}

func TestServiceRecordMutationsPreserveAuthorshipBoundary(t *testing.T) {
	// Rationale: Blueprint replacement must preserve runtime intent, while a
	// lifecycle mutation must preserve every desired field.
	record, err := NewServiceRecord(
		ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 2),
		serviceRecordTestDesired(),
		"",
	)
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	record, err = SetServiceRuntimeIntent(record, core.ServiceRuntimeIntentStopped)
	if err != nil {
		t.Fatalf("SetServiceRuntimeIntent() error = %v", err)
	}
	desired := record.Desired
	desired.Image = "app:next"
	replacement, err := ReplaceServiceDesired(record, desired)
	if err != nil {
		t.Fatalf("ReplaceServiceDesired() error = %v", err)
	}
	if replacement.Runtime != record.Runtime || replacement.Desired.Image != "app:next" {
		t.Fatalf("replacement = %#v", replacement)
	}
	if replacement.EnvironmentID != record.EnvironmentID {
		t.Fatalf("environment id changed from %q to %q", record.EnvironmentID, replacement.EnvironmentID)
	}
}

func TestServiceRecordEnvelopeRoundTripsStrictly(t *testing.T) {
	// Rationale: the first durable Service schema must reject corruption rather
	// than creating a compatibility reader that can reset operational state.
	record, err := NewServiceRecord(
		ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 3),
		serviceRecordTestDesired(),
		"",
	)
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	encoded, err := encodeServiceRecord(record)
	if err != nil {
		t.Fatalf("encodeServiceRecord() error = %v", err)
	}
	decoded, err := decodeServiceRecord(encoded)
	if err != nil {
		t.Fatalf("decodeServiceRecord() error = %v", err)
	}
	if decoded.EnvironmentID != record.EnvironmentID || decoded.Desired.ID != record.Desired.ID ||
		decoded.Runtime != record.Runtime {
		t.Fatalf("decoded = %#v, want %#v", decoded, record)
	}

	for name, corrupt := range map[string][]byte{
		"unknown":   bytes.Replace(encoded, []byte(`"runtime":`), []byte(`"unknown":0,"runtime":`), 1),
		"duplicate": bytes.Replace(encoded, []byte(`"runtime":`), []byte(`"desired":{},"runtime":`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeServiceRecord(corrupt); err == nil {
				t.Fatal("decodeServiceRecord() accepted corrupt record")
			}
		})
	}
}

// Rationale: an adapter-backed Service is the durable authority for the owner network every future Attach copies.
func TestBackingServiceRecordRequiresStableNetworkBinding(t *testing.T) {
	t.Parallel()
	desired := serviceRecordTestDesired()
	desired.Adapter = "postgres:16"
	environmentID := ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 6)
	if _, err := NewServiceRecord(environmentID, desired, ""); err == nil {
		t.Fatal("NewServiceRecord() accepted an adapter-backed Service without a backing network")
	}
	networkID := ids.NewAt(ids.KindNetwork, serviceRecordTestTime(), 7)
	record, err := NewServiceRecord(environmentID, desired, networkID)
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	if record.BackingNetworkID != networkID {
		t.Fatalf("BackingNetworkID = %q, want %q", record.BackingNetworkID, networkID)
	}
	desired.Image = "postgres:16.1-alpine"
	replacement, err := ReplaceServiceDesired(record, desired)
	if err != nil {
		t.Fatalf("ReplaceServiceDesired() error = %v", err)
	}
	if replacement.BackingNetworkID != networkID {
		t.Fatalf("ReplaceServiceDesired() changed backing network to %q", replacement.BackingNetworkID)
	}
}

func TestServiceRecordKeysUseStableOwnershipAndEncodedName(t *testing.T) {
	// Rationale: exact ADR 0013 keys are the atomic lookup and uniqueness
	// contract shared by direct mutations and Blueprint reconciliation.
	environmentID := ids.NewAt(ids.KindEnvironment, serviceRecordTestTime(), 4)
	serviceID := serviceRecordTestDesired().ID
	if got := serviceKey(serviceID); got != "/v1/records/services/"+serviceID {
		t.Fatalf("serviceKey() = %q", got)
	}
	if got := serviceOwnerKey(environmentID, serviceID); got !=
		"/v1/indexes/services/by-owner/environment/"+environmentID+"/"+serviceID {
		t.Fatalf("serviceOwnerKey() = %q", got)
	}
	if got := serviceNameKey(environmentID, "api/worker"); got !=
		"/v1/indexes/services/by-name/environment/"+environmentID+"/~YXBpL3dvcmtlcg" {
		t.Fatalf("serviceNameKey() = %q", got)
	}
}

func serviceRecordTestDesired() core.Service {
	return core.Service{
		ID: ids.NewAt(ids.KindService, serviceRecordTestTime(), 5), Name: "api", Image: "app:latest",
		Strategy: core.StrategyRecreate, OnFailure: core.OnFailureSwitchBack,
	}
}

func serviceRecordTestTime() time.Time {
	return time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
}
