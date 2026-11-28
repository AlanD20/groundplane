// Package runnercredential defines the authenticated identity of a Runner grant.
package runnercredential

import "time"

// Binding identifies one active credential without disclosing it. Revision is
// the credential record's modification revision. RequestID is empty for a
// Runner's explicitly granted CLI access and identifies a captured deployment
// request for an individual build job.
type Binding struct {
	RunnerID    string
	RequestID   string
	RunnerEpoch uint64
	Revision    int64
	ExpiresAt   time.Time
	EpochBound  bool
}

// ActiveAt checks the credential lifetime after the store proves the current
// Runner epoch. A migration base lasts for the lifetime of that epoch.
func (binding Binding) ActiveAt(now time.Time) bool {
	return binding.EpochBound && binding.RequestID == "" && binding.ExpiresAt.IsZero() ||
		!binding.EpochBound && binding.ExpiresAt.After(now)
}
