package runtimeconfiguration

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// WorkConditions loads captured sources at the admission revision and returns
// compares for the same transaction that assigns the work. It never substitutes
// newer configuration when the acknowledged predecessor has changed.
func (repository *Repository) WorkConditions(ctx context.Context, capture WorkCapture,
	revision int64,
) ([]Condition, error) {
	if ctx == nil || repository == nil || repository.store == nil ||
		capture.Validate() != nil || revision < capture.ReadRevision {
		return nil, errs.New(errs.KindValidationFailed, "configuration admission requires a captured storage revision")
	}
	keys := []string{HeadKey(capture.Current.EnvironmentID), publishedRootKey(capture.Current.ID)}
	references := []Reference{capture.Current}
	if capture.Prior != nil && *capture.Prior != capture.Current {
		if capture.Prior.ID == capture.Current.ID {
			return nil, errs.New(errs.KindStateConflict, "configuration identity has conflicting captured sources")
		}
		keys = append(keys, publishedRootKey(capture.Prior.ID))
		references = append(references, *capture.Prior)
	}
	read, err := repository.read(ctx, keys, revision)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, value := range read.Values {
			if value != nil {
				clear(value.Value)
			}
		}
	}()
	head := read.Values[0]
	if head == nil {
		if capture.Prior != nil {
			return nil, errs.New(errs.KindStateConflict, "acknowledged configuration was removed before execution")
		}
	} else {
		if capture.Prior == nil || head.ModRevision != capture.PriorRevision {
			return nil, errs.New(errs.KindStateConflict, "acknowledged configuration changed before execution")
		}
		prior, err := DecodeReference(head.Value)
		if err != nil || prior != *capture.Prior {
			return nil, errs.New(errs.KindStateConflict, "acknowledged configuration differs from the captured predecessor")
		}
	}
	conditions := []Condition{{Key: keys[0], ModRevision: capture.PriorRevision}}
	for index, reference := range references {
		root := read.Values[index+1]
		if root == nil {
			return nil, errs.New(errs.KindStateConflict, "captured configuration source is no longer retained")
		}
		if _, err := repository.Load(ctx, reference, revision); err != nil {
			return nil, err
		}
		conditions = append(conditions, Condition{Key: root.Key, ModRevision: root.ModRevision})
	}
	return conditions, nil
}
