package backuppolicymutations

import (
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Conditions exposes the prepared compare sequence for immediate publication.
// The returned slice is borrowed and must not be modified.
func (plan backupPolicyReplacementPlan) Conditions() []etcdstore.Condition {
	return plan.conditions
}

// Mutations exposes the prepared write sequence for immediate publication.
// The values are borrowed until Clear; the publisher copies them into its plan.
func (plan backupPolicyReplacementPlan) Mutations() []etcdstore.Mutation {
	return plan.mutations
}

func (plan backupPolicyReplacementPlan) OperationCount(marker idempotencyrecord.IdempotencyMarker) int {
	return backupPolicyReplacementOperationCount(plan, marker)
}

func (plan backupPolicyReplacementPlan) ClassifyConflict(values []*etcdstore.KeyValue) error {
	return ClassifyBackupPolicyReplacementConflict(values, plan.evidence)
}

func (plan backupPolicyReplacementPlan) Clear() {
	etcdstore.ClearMutationValues(plan.mutations)
}
