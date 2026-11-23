package monitoring

import metrics "github.com/AlanD20/groundplane/internal/common/monitoring"

// A limit present in every source slot covers every valid usage sample in
// that bucket. For a rolled-up bucket, the limit must also be constant to
// recover utilization extrema without pairing unrelated measurements.
func workloadCPUUtilization(usage, limit metrics.MetricSummary, slots uint32) metrics.MetricSummary {
	if slots == 0 || usage.Valid == 0 || usage.Valid > slots || limit.Valid != slots ||
		limit.Minimum == nil || limit.Maximum == nil || *limit.Minimum <= 0 ||
		*limit.Minimum != *limit.Maximum || !finite(*limit.Minimum) {
		return metrics.MetricSummary{}
	}
	divide := func(value *float64) *float64 {
		if value == nil {
			return nil
		}
		ratio := *value / *limit.Minimum
		if !finite(ratio) {
			return nil
		}
		return &ratio
	}
	usage.Average = divide(usage.Average)
	usage.Minimum = divide(usage.Minimum)
	usage.Maximum = divide(usage.Maximum)
	if usage.Average == nil {
		return metrics.MetricSummary{}
	}
	return usage
}
