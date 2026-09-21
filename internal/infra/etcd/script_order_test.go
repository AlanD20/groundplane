package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
)

// Rationale: numeric ordering is mutable metadata, not immutable body content;
// storage round trips and resetting to zero must preserve the original generation.
func TestScriptOrderRoundTripAndMetadataOnlyReplacement(t *testing.T) {
	record, err := testscripts.NewRecord(ids.New(ids.KindEnvironment), ids.New(ids.KindService), core.Script{
		ID: ids.New(ids.KindScript), Slug: "migrate", ServiceName: "api", Body: "echo migrate",
		When: core.ScriptPreDeploy,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, order := range []uint16{65535, 10, 0} {
		desired := record.Desired
		desired.Order = order
		replacement, err := testscripts.ReplaceDesired(record, desired)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := testscripts.EncodeRecord(replacement)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := testscripts.DecodeRecord(encoded)
		if err != nil || decoded.Desired.Order != order || decoded.ActiveGeneration != 1 {
			t.Fatalf("round trip order=%d: %#v, %v", order, decoded, err)
		}
		if decoded.Desired.Body != "" || replacement.Desired.Body != record.Desired.Body {
			t.Fatal("metadata replacement changed body ownership")
		}
		record = replacement
	}
}
