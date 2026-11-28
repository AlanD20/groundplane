// Package buildcredentials grants short-lived private registry access to
// verified GitHub build jobs running on scoped Groundplane Runners.
package buildcredentials

import (
	"context"
	"net/netip"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/controller/gitdelivery"
	"github.com/AlanD20/groundplane/internal/controller/runneridentity"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnercredential"
	"github.com/AlanD20/groundplane/internal/infra/githubapp"
	"github.com/AlanD20/groundplane/internal/infra/runnerregistry"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	credentialLifetime = 10 * time.Minute
	audiencePrefix     = "groundplane-build:"
)

type JobAuthority interface {
	CaptureRegistryJob(context.Context, string) (gitdelivery.RegistryJobSnapshot, error)
	VerifyRegistryJob(context.Context, gitdelivery.Run, string, *githubapp.OIDCProof) ([]keyvalue.Condition, error)
}

type ProofVerifier interface {
	Verify(context.Context, string, string) (githubapp.OIDCProof, error)
}

type Service struct {
	store       keyvalue.Store
	credentials *runnercredential.Repository
	jobs        JobAuthority
	verifier    ProofVerifier
	registry    netip.AddrPort
}

var _ runneridentity.RegistryCredentialExchange = (*Service)(nil)
var _ runnerregistry.JobAuthorizer = (*Service)(nil)

func New(
	store keyvalue.Store,
	credentials *runnercredential.Repository,
	jobs JobAuthority,
	verifier ProofVerifier,
	registry netip.AddrPort,
) (*Service, error) {
	if store == nil || credentials == nil || jobs == nil || verifier == nil ||
		!registry.Addr().Is4() || !registry.Addr().IsPrivate() || registry.Port() == 0 {
		return nil, errs.New(errs.KindValidationFailed, "scoped build registry dependencies are invalid")
	}
	return &Service{store: store, credentials: credentials, jobs: jobs, verifier: verifier, registry: registry}, nil
}

func (service *Service) IssueRegistryCredential(
	ctx context.Context,
	principal runneridentity.Principal,
	id string,
	token string,
) (runneridentity.RegistryCredential, error) {
	base := principal.Credential
	if base.RequestID != "" || ids.Validate(ids.KindOperation, id) != nil || !base.ActiveAt(time.Now()) {
		return runneridentity.RegistryCredential{}, denied()
	}
	proof, err := service.verifier.Verify(ctx, token, audiencePrefix+id)
	if err != nil {
		return runneridentity.RegistryCredential{}, denied()
	}
	snapshot, conditions, err := service.capture(ctx, id, base.RunnerID, &proof)
	if err != nil {
		return runneridentity.RegistryCredential{}, err
	}
	repository, err := registryRepository(snapshot.Run)
	if err != nil {
		return runneridentity.RegistryCredential{}, err
	}
	baseConditions, err := service.credentials.AdmissionConditions(ctx, principal.Digest, base)
	if err != nil {
		return runneridentity.RegistryCredential{}, err
	}
	conditions, err = mergeConditions(conditions, baseConditions)
	if err != nil {
		return runneridentity.RegistryCredential{}, err
	}
	now := time.Now().UTC()
	if !proof.ExpiresAt.After(now) || !base.ActiveAt(now) {
		return runneridentity.RegistryCredential{}, denied()
	}
	password, digest, err := mintPassword()
	if err != nil {
		return runneridentity.RegistryCredential{}, err
	}
	expires := now.Add(credentialLifetime)
	if !base.EpochBound && base.ExpiresAt.Before(expires) {
		expires = base.ExpiresAt
	}
	binding, err := service.credentials.Issue(ctx, runnercredential.IssueRequest{
		Digest: digest, RunnerID: base.RunnerID, RunnerEpoch: base.RunnerEpoch, RequestID: id,
		ExpiresAt: expires, Authority: conditions,
	})
	if err != nil {
		return runneridentity.RegistryCredential{}, err
	}
	return runneridentity.RegistryCredential{
		Username: id, Password: password, Repository: service.registry.String() + "/" + repository,
		ExpiresAt: binding.ExpiresAt,
	}, nil
}

// AuthorizeRegistryJob rechecks the job, Runner and connection before each
// private registry request. The password is independently scoped and is never
// accepted by any Runner API route.
func (service *Service) AuthorizeRegistryJob(
	ctx context.Context,
	username, password string,
) (runnerregistry.Grant, error) {
	digest, ok := parsePassword(password)
	if !ok || ids.Validate(ids.KindOperation, username) != nil {
		return runnerregistry.Grant{}, denied()
	}
	credential, err := service.credentials.ReadRunnerCredential(ctx, digest)
	if err != nil || credential.RequestID != username || credential.Revision <= 0 || credential.EpochBound {
		return runnerregistry.Grant{}, denied()
	}
	snapshot, conditions, err := service.capture(ctx, username, credential.RunnerID, nil)
	if err != nil {
		return runnerregistry.Grant{}, denied()
	}
	credentialConditions, err := service.credentials.AdmissionConditions(ctx, digest, credential)
	if err != nil {
		return runnerregistry.Grant{}, denied()
	}
	conditions, err = mergeConditions(conditions, credentialConditions)
	if err != nil || service.checkConditions(ctx, conditions) != nil || !credential.ActiveAt(time.Now()) {
		return runnerregistry.Grant{}, denied()
	}
	repository, err := registryRepository(snapshot.Run)
	if err != nil {
		return runnerregistry.Grant{}, denied()
	}
	return runnerregistry.Grant{
		Subject: username, Repository: repository, Actions: runnerregistry.Pull | runnerregistry.Push,
		ExpiresAt: credential.ExpiresAt,
	}, nil
}

func (service *Service) capture(
	ctx context.Context,
	id, runnerID string,
	proof *githubapp.OIDCProof,
) (gitdelivery.RegistryJobSnapshot, []keyvalue.Condition, error) {
	snapshot, err := service.jobs.CaptureRegistryJob(ctx, id)
	if err != nil {
		return gitdelivery.RegistryJobSnapshot{}, nil, err
	}
	if snapshot.Run.ID != id || len(snapshot.Conditions) == 0 {
		return gitdelivery.RegistryJobSnapshot{}, nil, denied()
	}
	runnerConditions, err := service.jobs.VerifyRegistryJob(ctx, snapshot.Run, runnerID, proof)
	if err != nil {
		return gitdelivery.RegistryJobSnapshot{}, nil, err
	}
	if len(runnerConditions) == 0 {
		return gitdelivery.RegistryJobSnapshot{}, nil, denied()
	}
	conditions, err := mergeConditions(snapshot.Conditions, runnerConditions)
	return snapshot, conditions, err
}

func registryRepository(run gitdelivery.Run) (string, error) {
	repository, ok := strings.CutPrefix(run.ImageRepository(), imagefetch.RegistryAuthority+"/")
	if !ok || repository == "" {
		return "", denied()
	}
	return repository, nil
}

func denied() *errs.Error {
	return errs.New(errs.KindRequestForbidden, "build job is not authorized")
}
