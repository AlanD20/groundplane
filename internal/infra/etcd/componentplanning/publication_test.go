package componentplanning

import (
	"errors"
	"testing"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestComponentPublicationClassifiesDesiredAndAppliedProjectionFences(t *testing.T) {
	t.Parallel()
	publication := Publication{
		preparation: ComponentTaskPreparation{
			desiredProjectionRevision: 9,
			appliedProjectionPresent:  true,
			appliedProjectionRevision: 7,
		},
		conditions: []testkeyvalue.Condition{
			{Key: "active"},
			{Key: "intent"},
			{Key: "applied", ModRevision: 7},
			{Key: "head", ModRevision: 9},
		},
	}
	valid := []*testkeyvalue.KeyValue{
		nil,
		nil,
		{Key: "applied", ModRevision: 7},
		{Key: "head", ModRevision: 9},
	}
	if err := publication.Classify(valid); err != nil {
		t.Fatalf("Classify(equal fences) error = %v", err)
	}
	for _, test := range []struct {
		name  string
		index int
		value *testkeyvalue.KeyValue
	}{
		{name: "applied removed", index: 2},
		{name: "applied advanced", index: 2, value: &testkeyvalue.KeyValue{Key: "applied", ModRevision: 8}},
		{name: "desired advanced", index: 3, value: &testkeyvalue.KeyValue{Key: "head", ModRevision: 10}},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := append([]*testkeyvalue.KeyValue(nil), valid...)
			values[test.index] = test.value
			if err := publication.Classify(values); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				t.Fatalf("Classify() error = %v, want state conflict", err)
			}
		})
	}
}
