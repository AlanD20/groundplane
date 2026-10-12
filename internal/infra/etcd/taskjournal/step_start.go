package taskjournal

// StepStartDecision is execution authority, not an observation. Wait and Stop
// acknowledge a request without recording a Running event or permitting effects.
type StepStartDecision uint8

const (
	StepStartNotRequested StepStartDecision = iota
	StepStartAllowed
	StepStartWait
	StepStartStop
)
