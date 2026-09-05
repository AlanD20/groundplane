package release

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestGroupRollbackInputPreservesPresenceAndRejectsInvalidValues(t *testing.T) {
	tag, blank, revision := " release ", "", int64(0)
	for _, input := range []GroupRollbackInput{{Tag: &tag}, {Tag: &blank}, {PreviewRevision: &revision}} {
		if _, err := NewGroupRollbackInput(input.Tag, input.PreviewRevision); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("NewGroupRollbackInput(%#v) error = %v", input, err)
		}
	}
	exact, revision := "release tag", int64(42)
	input, err := NewGroupRollbackInput(&exact, &revision)
	if err != nil || input.Tag == nil || *input.Tag != exact || input.PreviewRevision == nil || *input.PreviewRevision != revision {
		t.Fatalf("NewGroupRollbackInput exact = %#v, %v", input, err)
	}
}
