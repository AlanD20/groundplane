package etcd

import "github.com/AlanD20/groundplane/pkg/errs"

// Hook Attach and backing topology evidence can overlap the owning Blueprint's
// own Attach comparisons. Preserve one equal comparison, never weaken a
// conflicting revision to make the transaction fit.
func (publication BlueprintReleasePublication) withExistingComparisons(
	existing []Condition,
) (BlueprintReleasePublication, error) {
	seen := make(map[string]Condition, len(existing)+len(publication.conditions))
	for _, condition := range existing {
		seen[condition.Key] = condition
	}
	remaining := make([]Condition, 0, len(publication.conditions))
	for _, condition := range publication.conditions {
		if previous, found := seen[condition.Key]; found {
			if previous != condition {
				return BlueprintReleasePublication{}, errs.New(
					errs.KindStateConflict,
					"Blueprint release source revisions disagree",
				)
			}
			continue
		}
		seen[condition.Key] = condition
		remaining = append(remaining, condition)
	}
	publication.conditions = remaining
	return publication, nil
}
