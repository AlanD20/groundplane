package etcd

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
)

// Rationale: context is mutable metadata; explicit grants and inherited reset
// survive storage without creating a generation or moving body bytes into it.
func TestScriptContextRoundTripPreservesBodyGeneration(t *testing.T) {
	record, err := testscripts.NewRecord(ids.New(ids.KindEnvironment), ids.New(ids.KindService), core.Script{
		ID: ids.New(ids.KindScript), Slug: "initialize", ServiceName: "api", Body: "echo initialize",
		When: core.ScriptPreDeploy,
	})
	if err != nil {
		t.Fatal(err)
	}
	explicit := &core.ScriptExecution{
		Mode: core.ScriptExecutionExplicit, Image: "example/setup@sha256:" + strings.Repeat("b", 64), User: "0:0",
		Volumes:  []core.ScriptVolumeGrant{{VolumeID: ids.New(ids.KindVolume), Target: "/storage", ReadOnly: false}},
		EntryIDs: []string{ids.New(ids.KindEnvEntry)},
	}
	for _, execution := range []*core.ScriptExecution{explicit, {Mode: core.ScriptExecutionInherited}, nil} {
		desired := record.Desired
		desired.Execution = execution
		replacement, err := testscripts.ReplaceDesired(record, desired)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := testscripts.EncodeRecord(replacement)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := testscripts.DecodeRecord(encoded)
		if err != nil || decoded.ActiveGeneration != record.ActiveGeneration || decoded.Desired.Body != "" ||
			replacement.Desired.Body != record.Desired.Body {
			t.Fatalf("context edit changed body ownership: %v", err)
		}
		got := decoded.Desired.Execution
		if execution == nil {
			if got != nil {
				t.Fatal("omitted inherited context was not preserved")
			}
		} else if got == nil || got.Mode != execution.Mode || got.Image != execution.Image ||
			got.User != execution.User || len(got.Volumes) != len(execution.Volumes) || len(got.EntryIDs) != len(execution.EntryIDs) {
			t.Fatal("context metadata changed during storage round trip")
		} else if execution.Mode == core.ScriptExecutionExplicit &&
			(got.Volumes[0] != execution.Volumes[0] || got.EntryIDs[0] != execution.EntryIDs[0]) {
			t.Fatal("exact grant changed during storage round trip")
		}
		record = replacement
	}
}

// Rationale: malformed context must fail the durable write boundary as well as
// its eventual human-facing decoder; a typed internal caller is not authority.
func TestScriptContextRecordRejectsMalformedMetadata(t *testing.T) {
	_, err := testscripts.NewRecord(ids.New(ids.KindEnvironment), ids.New(ids.KindService), core.Script{
		ID: ids.New(ids.KindScript), Slug: "initialize", ServiceName: "api", Body: "echo initialize",
		When: core.ScriptPreDeploy, Execution: &core.ScriptExecution{Mode: core.ScriptExecutionExplicit},
	})
	if err == nil {
		t.Fatal("durable Script accepted an incomplete explicit context")
	}
}
