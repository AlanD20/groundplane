package scriptsourcepublication

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type sourceStore interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

func (prepared PreparedSourceSet) MembershipCount() uint64  { return prepared.membershipCount }
func (prepared PreparedSourceSet) MembershipSHA256() string { return prepared.membershipSHA256 }

// Conditions returns borrowed compares; callers copy before extending them.
func (fragment ScriptSourcePublicationFragment) Conditions() []etcdstore.Condition {
	return fragment.conditions
}

// Mutations returns borrowed operations whose bytes remain owned by Clear.
func (fragment ScriptSourcePublicationFragment) Mutations() []etcdstore.Mutation {
	return fragment.mutations
}

// Conditions returns borrowed compares; callers copy before extending them.
func (fragment ScriptSourceReleaseFragment) Conditions() []etcdstore.Condition {
	return fragment.conditions
}

// Mutations returns borrowed operations whose bytes remain owned by Clear.
func (fragment ScriptSourceReleaseFragment) Mutations() []etcdstore.Mutation {
	return fragment.mutations
}
