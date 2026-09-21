package entry

import (
	"testing"

	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// QA: ENT-11; local removal selection.
// Rationale: an absent Entry must not acquire host cleanup authority merely
// because an Environment projection exists.
func TestAppliedEntryProjectionExcludesEntryAbsentFromCurrentProjection(t *testing.T) {
	var projection testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]

	if got := appliedEntryProjection(projection, true, "ev_01M141W885A4MFM98WJ3AW2T21"); got != nil {
		t.Fatal("expected an entry absent from the current projection to be classified as never applied")
	}
}
