package monitoring

import (
	"math"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Window struct {
	Public           api.MonitoringWindow
	SourceResolution time.Duration
	GroupSize        int
}

// HistoryWindow closes the source tier before querying. Each output bucket has
// explicit bounds; a partial final bucket never claims the nominal full width.
func HistoryWindow(now time.Time, selected string, points int) (Window, error) {
	if now.IsZero() || points < 0 || points > metrics.MaximumQueryPoints || points > 0 && points < 16 {
		return Window{}, errs.New(errs.KindValidationFailed, "monitoring points must be between 16 and 720")
	}
	if points == 0 {
		points = 240
	}
	if selected == "" {
		selected = "1h"
	}
	var span, resolution time.Duration
	switch selected {
	case "1h":
		span, resolution = time.Hour, metrics.SampleInterval
	case "6h":
		span, resolution = 6*time.Hour, metrics.MediumResolution
	case "24h":
		span, resolution = 24*time.Hour, metrics.MediumResolution
	case "7d":
		span, resolution = 7*24*time.Hour, metrics.CoarseResolution
	default:
		return Window{}, errs.New(errs.KindValidationFailed, "monitoring range must be 1h, 6h, 24h or 7d")
	}
	end := now.UTC().Truncate(resolution)
	group := (int(span/resolution) + points - 1) / points
	return Window{
		Public: api.MonitoringWindow{
			Range:         selected,
			Start:         end.Add(-span),
			End:           end,
			BucketSeconds: uint32(time.Duration(group) * resolution / time.Second),
		},
		SourceResolution: resolution,
		GroupSize:        group,
	}, nil
}

// CombineMetric summarizes retained observed samples. An unknown extremum on
// any valid input remains unknown; it is not fabricated from an average.
func CombineMetric(values []metrics.MetricSummary) (metrics.MetricSummary, error) {
	var result metrics.MetricSummary
	var weighted float64
	missingMin, missingMax := false, false
	for _, value := range values {
		if uint64(result.ContributorExpected)+uint64(value.ContributorExpected) > 1<<32-1 ||
			uint64(result.ContributorValid)+uint64(value.ContributorValid) > 1<<32-1 ||
			value.ContributorValid > value.ContributorExpected ||
			uint64(result.Expected)+uint64(value.Expected) > 1<<32-1 ||
			uint64(result.Valid)+uint64(value.Valid) > 1<<32-1 ||
			value.Valid > value.Expected {
			return metrics.MetricSummary{}, errs.New(
				errs.KindStorageUnavailable,
				"monitoring sample counts are invalid",
			)
		}
		result.Expected += value.Expected
		result.Valid += value.Valid
		result.ContributorValid += value.ContributorValid
		result.ContributorExpected += value.ContributorExpected
		if value.Valid == 0 {
			continue
		}
		if value.Average == nil || !finite(*value.Average) ||
			value.Minimum != nil && (!finite(*value.Minimum) || *value.Minimum > *value.Average) ||
			value.Maximum != nil && (!finite(*value.Maximum) || *value.Maximum < *value.Average) {
			return metrics.MetricSummary{}, errs.New(
				errs.KindStorageUnavailable,
				"monitoring average is missing for valid samples",
			)
		}
		weighted += *value.Average * float64(value.Valid)
		if !finite(weighted) {
			return metrics.MetricSummary{}, errs.New(errs.KindStorageUnavailable, "monitoring aggregate overflow")
		}
		if value.Minimum == nil {
			missingMin = true
		} else if result.Minimum == nil || *value.Minimum < *result.Minimum {
			copy := *value.Minimum
			result.Minimum = &copy
		}
		if value.Maximum == nil {
			missingMax = true
		} else if result.Maximum == nil || *value.Maximum > *result.Maximum {
			copy := *value.Maximum
			result.Maximum = &copy
		}
	}
	if result.Valid > 0 {
		average := weighted / float64(result.Valid)
		result.Average = &average
	}
	if missingMin {
		result.Minimum = nil
	}
	if missingMax {
		result.Maximum = nil
	}
	return result, nil
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }
