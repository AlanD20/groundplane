package attachexecution

import "time"

// RemovalBudget preserves the ordinary Detach budget while allowing a captured,
// validated Custom hook its configured execution time plus termination overhead.
func RemovalBudget(customTimeoutSeconds int64) time.Duration {
	return max(2*time.Minute, time.Duration(customTimeoutSeconds+15)*time.Second)
}
