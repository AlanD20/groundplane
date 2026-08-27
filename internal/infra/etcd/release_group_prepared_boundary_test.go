package etcd_test

import (
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: callers must not gain raw etcd compare, key, value, mutation-type, prefix, or seal authority from prepared fragments.
func TestReleaseGroupPreparedFragmentsExposeNoTransactionAuthority(t *testing.T) {
	t.Parallel()
	for _, value := range []any{
		etcd.ReleaseGroupPreparedMutation{},
		etcd.ReleaseGroupBlueprintPreparedMutation{},
	} {
		kind := reflect.TypeOf(value)
		for index := 0; index < kind.NumField(); index++ {
			field := kind.Field(index)
			if field.PkgPath == "" {
				t.Fatalf("%s exposes field %q", kind.Name(), field.Name)
			}
		}
	}
}
