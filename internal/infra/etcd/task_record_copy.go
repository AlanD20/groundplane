package etcd

import (
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"
)

func parseOptionalTimestamp(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := recordcodec.ParseCanonicalTimestamp(value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func formatOptionalTimestamp(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func taskContainsStep(task TaskRecord, stepID string) bool {
	for _, step := range task.Steps {
		if step.ID == stepID {
			return true
		}
	}
	return false
}

func cloneIdempotencyLocator(locator *idempotencyrecord.IdempotencyLocator) *idempotencyrecord.IdempotencyLocator {
	if locator == nil {
		return nil
	}
	cloned := *locator
	return &cloned
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func timePointer(value time.Time) *time.Time {
	return &value
}
