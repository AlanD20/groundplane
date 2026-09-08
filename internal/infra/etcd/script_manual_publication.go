package etcd

import "github.com/AlanD20/groundplane/pkg/errs"

// manualScriptSourceConditions fences the desired sources captured for a manual
// execution. Release image and render-input fences are owned by its publication.
func manualScriptSourceConditions(sources ScriptExecutionSources) ([]Condition, error) {
	conditions := []Condition{serviceDesiredCondition(sources.Service)}
	byKey := map[string]Condition{conditions[0].Key: conditions[0]}
	for _, condition := range scriptExecutionProjectionConditions(sources) {
		if existing, found := byKey[condition.Key]; found {
			if existing != condition {
				return nil, errs.New(errs.KindStateConflict, "Script execution source revisions disagree")
			}
			continue
		}
		byKey[condition.Key] = condition
		conditions = append(conditions, condition)
	}
	return conditions, nil
}

func classifyScriptExecutionPublication(expected int) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		if len(values) != expected {
			return errs.New(errs.KindInternal, "Script execution publication compare evidence is incomplete")
		}
		if values[6] != nil {
			return errs.New(errs.KindStateConflict, "Script operation already has an active Task")
		}
		for index, value := range values {
			if index < 8 && value != nil {
				return errs.New(errs.KindInternal, "Script execution identity collided with durable state")
			}
			if index >= 8 && value == nil {
				return errs.New(errs.KindStateConflict, "Script execution source changed before publication")
			}
		}
		return errs.New(errs.KindStateConflict, "Script execution source changed before publication")
	}
}
