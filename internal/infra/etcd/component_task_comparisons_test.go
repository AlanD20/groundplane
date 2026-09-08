package etcd

import (
	"errors"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestComponentPublicationSharedCompareRetainsBothClassifiers(t *testing.T) {
	publication := preparedComponentTaskPublication{
		preparation: ComponentTaskPreparation{
			desiredProjectionRevision: 9,
			appliedProjectionPresent:  true,
			appliedProjectionRevision: 7,
		},
		conditions: []Condition{
			{Key: "active"},
			{Key: "intent"},
			{Key: "applied", ModRevision: 7},
			{Key: "head", ModRevision: 9},
		},
	}
	frozen := append([]Condition(nil), publication.conditions...)
	base := []Condition{{Key: "head", ModRevision: 9}}
	baseCalled := false
	merged, classify, err := composeEnvironmentBlueprintComponentPublication(
		base,
		func(_ int64, values []*KeyValue) error {
			baseCalled = true
			if len(values) != 1 || values[0].Key != "head" {
				t.Fatalf("base classifier lost its compare: %#v", values)
			}
			return nil
		},
		publication,
	)
	if err != nil || len(merged) != 4 || !reflect.DeepEqual(publication.conditions, frozen) || len(base) != 1 {
		t.Fatalf("shared comparison composition changed authority: %#v, %v", merged, err)
	}
	values := []*KeyValue{{Key: "head", ModRevision: 9}, nil, nil, {Key: "applied", ModRevision: 7}}
	if err := classify(10, values); err != nil || !baseCalled {
		t.Fatalf("equal shared evidence failed classification: %v", err)
	}
	values[0].ModRevision = 10
	if err := classify(10, values); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Component classifier lost shared head fence: %v", err)
	}
	for _, conflict := range []Condition{{Key: "head", ModRevision: 8}, {Key: "head", ModRevision: 9, Prefix: true}} {
		if _, _, err := composeEnvironmentBlueprintComponentPublication([]Condition{conflict}, nil, publication); !errors.Is(
			err,
			errs.New(errs.KindStateConflict, ""),
		) {
			t.Fatalf("unequal shared comparison accepted: %v", err)
		}
	}
}
