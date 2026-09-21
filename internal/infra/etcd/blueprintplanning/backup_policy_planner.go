package blueprintplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type blueprintReadStore interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

type BackupPolicyPlanner struct {
	store  blueprintReadStore
	policy *backuppolicymutations.Repository
}

func NewBackupPolicyPlanner(store blueprintReadStore, policy *backuppolicymutations.Repository) *BackupPolicyPlanner {
	return &BackupPolicyPlanner{store: store, policy: policy}
}

// TaskIdentity is the existing Task identity consumed by Blueprint preparation.
type TaskIdentity struct{ ID, Target string }

// Conditions and Mutations borrow buffers until the enclosing publication clears them.
func (publication AttachPublication) Conditions() []keyvalue.Condition { return publication.conditions }
func (publication AttachPublication) Mutations() []keyvalue.Mutation   { return publication.mutations }
func (publication BackupPolicyPublication) Conditions() []keyvalue.Condition {
	return publication.conditions
}
func (publication BackupPolicyPublication) Mutations() []keyvalue.Mutation {
	return publication.mutations
}
