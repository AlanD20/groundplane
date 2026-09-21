package etcd

import (
	"context"
	"errors"
	"reflect"
	"testing"

	testcomponentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestComponentPublicationSharedCompareRetainsBothClassifiers(t *testing.T) {
	publication := zeroComponentPublication(t)
	frozen := append([]testkeyvalue.Condition(nil), publication.Conditions()...)
	if len(frozen) != 1 {
		t.Fatalf("zero Component publication conditions = %#v", frozen)
	}
	base := append([]testkeyvalue.Condition(nil), frozen...)
	baseCalled := false
	merged, classify, err := composeEnvironmentBlueprintComponentPublication(
		base,
		func(_ int64, values []*testkeyvalue.KeyValue) error {
			baseCalled = true
			if len(values) != 1 {
				t.Fatalf("base classifier lost its compare: %#v", values)
			}
			return nil
		},
		publication,
	)
	if err != nil || len(merged) != 1 || !reflect.DeepEqual(publication.Conditions(), frozen) || len(base) != 1 {
		t.Fatalf("shared comparison composition changed authority: %#v, %v", merged, err)
	}
	values := []*testkeyvalue.KeyValue{nil}
	if err := classify(10, values); err != nil || !baseCalled {
		t.Fatalf("equal shared evidence failed classification: %v", err)
	}
	values[0] = &testkeyvalue.KeyValue{Key: frozen[0].Key, ModRevision: 10}
	if err := classify(10, values); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Component classifier lost shared active-task fence: %v", err)
	}
	for _, conflict := range []testkeyvalue.Condition{
		{Key: frozen[0].Key, ModRevision: 8},
		{Key: frozen[0].Key, Prefix: true},
	} {
		if _, _, err := composeEnvironmentBlueprintComponentPublication([]testkeyvalue.Condition{conflict}, nil, publication); !errors.Is(
			err,
			errs.New(errs.KindStateConflict, ""),
		) {
			t.Fatalf("unequal shared comparison accepted: %v", err)
		}
	}
}

func zeroComponentPublication(t *testing.T) testcomponentplanning.Publication {
	t.Helper()
	publication, err := testcomponentplanning.NewPlanner(nil).PrepareComponentTaskPublication(
		context.Background(),
		testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{ID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		},
		testcomponentplanning.TaskIdentity{},
		nil,
		testcomponentplanning.ComponentTaskPreparation{},
	)
	if err != nil {
		t.Fatalf("PrepareComponentTaskPublication(zero) error = %v", err)
	}
	return publication
}
