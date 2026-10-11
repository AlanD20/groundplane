package rollingrelease

import "time"

// StepBudget is selected from the durable cursor after accepting a phase start.
// Delivery, readiness updates and subsequent phases do not restart its deadline.
type StepBudget struct {
	Deadline   time.Time
	DrainUntil time.Time
}
