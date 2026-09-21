package etcd

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
)

func TestComponentRecordSeparatesDesiredAndRuntimeReplacement(t *testing.T) {
	// Rationale: config edits must not overwrite generated Service identity,
	// pinned addressing, or observed health owned by the Controller.
	t.Parallel()
	record := componentRecordTestRecord(t, 1001)
	component, err := testcomponents.ProjectRecord(record)
	if err != nil {
		t.Fatalf("ProjectComponentRecord() error = %v", err)
	}
	component.Config.Caddy.CaddyfileTemplate = "{gp.routes}\n"
	replacement, err := testcomponents.ReplaceDesired(record, component)
	if err != nil {
		t.Fatalf("ReplaceComponentDesired() error = %v", err)
	}
	if !reflect.DeepEqual(replacement.Runtime, record.Runtime) {
		t.Fatalf("runtime = %#v, want %#v", replacement.Runtime, record.Runtime)
	}
	updated, err := testcomponents.SetRuntime(
		replacement,
		[]string{component.GeneratedServices[0]},
		"10.40.10.3",
		false,
	)
	if err != nil {
		t.Fatalf("SetComponentRuntime() error = %v", err)
	}
	if !reflectComponentDesiredEqual(updated.Desired, replacement.Desired) ||
		updated.Runtime.PinnedIPv4 != "10.40.10.3" {
		t.Fatalf("updated = %#v", updated)
	}
}

func TestComponentRecordEnvelopeRoundTripsStrictly(t *testing.T) {
	// Rationale: component config and Controller-owned runtime identity must
	// fail closed rather than changing shape after durable decoding.
	t.Parallel()
	record := componentRecordTestRecord(t, 1002)
	encoded, err := testcomponents.EncodeRecord(record)
	if err != nil {
		t.Fatalf("encodeComponentRecord() error = %v", err)
	}
	decoded, err := testcomponents.DecodeRecord(encoded)
	if err != nil || !reflectComponentRecordEqual(decoded, record) {
		t.Fatalf("decodeComponentRecord() = %#v, %v, want %#v", decoded, err, record)
	}
	corrupt := bytes.Replace(encoded, []byte(`"runtime":`), []byte(`"unknown":0,"runtime":`), 1)
	if _, err := testcomponents.DecodeRecord(corrupt); err == nil {
		t.Fatal("decodeComponentRecord() accepted an unknown field")
	}
}

func componentRecordTestRecord(t *testing.T, offset int64) testcomponents.Record {
	t.Helper()
	at := serviceRecordTestTime()
	record, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, at, offset), Owner: core.ComponentOwnerEnvironment,
		OwnerID: ids.NewAt(ids.KindEnvironment, at, 2), Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{ids.NewAt(ids.KindNetwork, at, 3)},
		}},
		GeneratedServices: []string{ids.NewAt(ids.KindService, at, 4)},
		PinnedIPv4:        "10.40.10.2",
		Healthy:           true,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	return record
}

func reflectComponentDesiredEqual(left testcomponents.DesiredRecord, right testcomponents.DesiredRecord) bool {
	return reflectComponentRecordEqual(testcomponents.Record{Desired: left}, testcomponents.Record{Desired: right})
}

func reflectComponentRecordEqual(left testcomponents.Record, right testcomponents.Record) bool {
	leftValue, leftErr := testcomponents.EncodeRecord(left)
	rightValue, rightErr := testcomponents.EncodeRecord(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftValue, rightValue)
}
