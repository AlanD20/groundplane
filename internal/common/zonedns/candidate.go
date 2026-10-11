package zonedns

// Candidate names the current desired owner of one opted-in bridge. Its
// reserved gateway is a listener only after host inspection proves this bridge
// actually exists with that ownership, subnet and internal-network policy.
type Candidate struct {
	ZoneID        string
	EnvironmentID string
	Subnet        string
}
