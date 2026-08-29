package entry

import (
	"testing"

	infraetcd "github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestAppliedEntryProjectionExcludesEntryAbsentFromCurrentProjection(t *testing.T) {
	var projection infraetcd.Versioned[infraetcd.EnvironmentComposeProjection]

	if got := appliedEntryProjection(projection, true, "ev_01M141W885A4MFM98WJ3AW2T21"); got != nil {
		t.Fatal("expected an entry absent from the current projection to be classified as never applied")
	}
}
