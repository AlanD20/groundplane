// Package etcdactivation names host observations of one captured configuration
// activation. These observations do not grant another forward Apply.
package etcdactivation

type Outcome string

const (
	NotStarted Outcome = "not_started"
	Applied    Outcome = "applied"
	Restored   Outcome = "restored"
	Unresolved Outcome = "unresolved"
)

func (outcome Outcome) Valid() bool {
	return outcome == NotStarted || outcome == Applied || outcome == Restored || outcome == Unresolved
}
