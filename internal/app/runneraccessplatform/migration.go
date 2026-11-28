package runneraccessplatform

import (
	"context"

	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/controller/runner"
	"github.com/AlanD20/groundplane/internal/controller/runneridentity"
	dockerrunner "github.com/AlanD20/groundplane/internal/infra/docker/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnercredential"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerscoped"
	"github.com/AlanD20/groundplane/internal/infra/runnerregistry"
)

type stagedRunnerCredentials struct {
	repository *runnercredential.Repository
	cipher     runnercredential.StageCipher
	host       *dockerrunner.HostControl
}

func (credentials stagedRunnerCredentials) EnsureScopedCredential(
	ctx context.Context, migration keyvalue.Versioned[runnerscoped.Migration],
) error {
	plan := migration.Record.Candidate
	certificate, err := runnerregistry.LocalCertificate(ctx, plan.Egress.ScopedRegistry)
	if err != nil {
		return err
	}
	if err := credentials.host.InstallScopedRegistryTrust(ctx, plan, certificate); err != nil {
		return err
	}
	token, _, err := credentials.repository.StageScopedBase(ctx, migration, credentials.cipher, runneridentity.Mint)
	if err != nil {
		return err
	}
	return credentials.host.InstallScopedCredential(ctx, plan, []byte(token))
}

func (credentials stagedRunnerCredentials) StageCompletion(
	ctx context.Context, migration keyvalue.Versioned[runnerscoped.Migration],
) ([]keyvalue.Condition, keyvalue.Mutation, error) {
	return credentials.repository.StageCompletion(ctx, migration)
}

func NewMigration(
	store keyvalue.Store,
	cipher runnercredential.StageCipher,
	commands commandrunner.Runner,
	executor *runner.Executor,
) (*runner.ScopedMigration, error) {
	credentials, err := runnercredential.New(store)
	if err != nil {
		return nil, err
	}
	host, err := dockerrunner.NewLocal(commands)
	if err != nil {
		return nil, err
	}
	return runner.NewScopedMigration(runnerscoped.NewRepository(store), host,
		stagedRunnerCredentials{repository: credentials, cipher: cipher, host: host}, executor)
}
