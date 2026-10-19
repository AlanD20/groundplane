package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Removal combines resource, hierarchy, DNS and desired-identity guards. These
// owners can capture the same key (including the platform resolver) independently.
// Keep one exact comparison; disagreement is a stale capture, never permission
// to select one owner's revision or omit its protection.
func mergeServiceRemovalConditions(conditions []keyvalue.Condition) ([]keyvalue.Condition, error) {
	seen := make(map[string]keyvalue.Condition, len(conditions))
	merged := make([]keyvalue.Condition, 0, len(conditions))
	for _, condition := range conditions {
		if previous, found := seen[condition.Key]; found {
			if previous != condition {
				return nil, errs.New(errs.KindStateConflict, "Service removal sources changed during capture")
			}
			continue
		}
		seen[condition.Key] = condition
		merged = append(merged, condition)
	}
	return merged, nil
}
