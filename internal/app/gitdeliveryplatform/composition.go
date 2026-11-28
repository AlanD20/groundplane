// Package gitdeliveryplatform assembles the GitHub delivery operator.
package gitdeliveryplatform

import (
	"github.com/AlanD20/groundplane/internal/controller/gitdelivery"
	"github.com/AlanD20/groundplane/internal/controller/gitdeliveryoperator"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/githubconnection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type Operator = gitdeliveryoperator.Operator
type RunnerAuthority = gitdeliveryoperator.RunnerAuthority
type RunnerAuthorityEvidence = gitdeliveryoperator.RunnerAuthorityEvidence

func New(store keyvalue.Store, cipher githubconnection.Cipher, idempotency *requestidempotency.Coordinator,
	replays *etcd.IdempotencyRepository, releases gitdelivery.Releases, authorities ...RunnerAuthority,
) (*Operator, error) {
	connections, err := githubconnection.NewRepository(store, cipher)
	if err != nil {
		return nil, err
	}
	repository, err := gitdelivery.NewRepository(store)
	if err != nil {
		return nil, err
	}
	return gitdeliveryoperator.New(store, connections, repository, idempotency, replays, releases, authorities...)
}
