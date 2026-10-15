package composeinput

import (
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/compose-spec/compose-go/v2/types"
)

// Healthcheck retains every native check field in the editable
// desired model; Blueprint imports and direct edits use the same representation.
func Healthcheck(value *types.HealthCheckConfig) core.Healthcheck {
	if value == nil {
		return core.Healthcheck{}
	}
	result := core.Healthcheck{Test: append([]string(nil), value.Test...), Disable: value.Disable}
	for _, field := range []struct {
		source *types.Duration
		target *string
	}{
		{value.Interval, &result.Interval}, {value.Timeout, &result.Timeout},
		{value.StartPeriod, &result.StartPeriod}, {value.StartInterval, &result.StartInterval},
	} {
		if field.source != nil {
			*field.target = field.source.String()
		}
	}
	if value.Retries != nil {
		result.Retries = int(*value.Retries)
	}
	return result
}
