package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
)

// TaskAssignment contains both records created by one successful claim.
type TaskAssignment struct {
	Assignment             etcdstore.Versioned[taskassignments.TaskAssignmentRecord]
	Task                   etcdstore.Versioned[TaskRecord]
	ReleaseRecovery        *taskassignments.ReleaseRecoveryDirective
	ServiceAbsenceRecovery *taskassignments.ReleaseRecoveryDirective
	ManagedNetworkRecovery *taskassignments.ReleaseRecoveryDirective
	AttachRemovalRecovery  *taskassignments.ReleaseRecoveryDirective
	// RecoveryProofRequired is private lifecycle state. It is true only after
	// the immutable recovery deadline expired without exact restoration proof.
	RecoveryProofRequired bool
	// DispatchHeld means recovery needs explicit operator authorization. Reading
	// or reconnecting the assignment is not permission to create a new budget.
	DispatchHeld bool
}
