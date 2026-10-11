package dnsrecords

import model "github.com/AlanD20/groundplane/internal/common/dnsrecords"

// EnvironmentStatus is one revision-consistent read for authoring and runtime
// presentation. HeadRevision is a transaction precondition, not a public id.
type EnvironmentStatus struct {
	RevisionID      string
	HeadRevision    int64
	ResolverEnabled bool
	State           string
	TaskID          string
	Records         []EnvironmentRecordStatus
	Withdrawals     []EnvironmentWithdrawal
}

type EnvironmentRecordStatus struct {
	Binding model.EnvironmentBinding
	Spec    model.EnvironmentSpec
	State   string
	TaskID  string
}

type EnvironmentWithdrawal struct {
	RecordID string
	Hostname string
}
