package runnermigrationoperator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/gitdeliveryoperator"
	intent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/runner"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/githubconnection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerscoped"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	Route = "/runners/{id}/scoped-migration"
	Kind  = "runner-scoped-migration"
)

type Verification interface {
	VerifyRunnerRegistration(
		context.Context,
		string,
		string,
		int64,
	) (gitdeliveryoperator.RunnerAuthorityEvidence, error)
}

type Operator struct {
	records        *runnerscoped.Repository
	runners        *runners.Reader
	migration      *runner.ScopedMigration
	verification   Verification
	readiness      func(context.Context) error
	policy         corerunner.IsolationPolicy
	candidateImage string
	intents        *intent.Coordinator
	replays        *etcd.IdempotencyRepository
	submitter      *acceptedaction.Submitter
}

func New(
	records *runnerscoped.Repository,
	runners *runners.Reader,
	migration *runner.ScopedMigration,
	verification Verification,
	readiness func(context.Context) error,
	policy corerunner.IsolationPolicy,
	candidateImage string,
	intents *intent.Coordinator,
	replays *etcd.IdempotencyRepository,
	submitter *acceptedaction.Submitter,
) (*Operator, error) {
	if records == nil || runners == nil || migration == nil || verification == nil || readiness == nil ||
		intents == nil || replays == nil || submitter == nil || !policy.ScopedEndpoint.IsValid() ||
		!policy.ScopedRegistry.IsValid() || !imageref.IsDigestPinned(candidateImage) {
		return nil, errs.New(errs.KindInternal, "Scoped Runner migration dependencies are required")
	}
	return &Operator{records: records, runners: runners, migration: migration, verification: verification,
		readiness: readiness, policy: policy, candidateImage: candidateImage, intents: intents, replays: replays, submitter: submitter}, nil
}

func (operator *Operator) ReadScopedMigration(
	ctx context.Context,
	runnerID string,
) (api.RunnerScopedMigrationStatus, error) {
	current, err := operator.runners.GetRunner(ctx, runnerID)
	if err != nil {
		return api.RunnerScopedMigrationStatus{}, err
	}
	state, exists, err := operator.records.Load(ctx, runnerID)
	if err != nil {
		return api.RunnerScopedMigrationStatus{}, err
	}
	result := api.RunnerScopedMigrationStatus{RunnerID: runnerID, Revision: current.Revision, Phase: "legacy"}
	if !exists {
		if current.Record.ProvisioningState == runners.RunnerProvisioningReady {
			registrationOnly, err := operator.records.RegistrationOnlyAt(ctx, runnerID, current.ReadRevision)
			if err != nil {
				return api.RunnerScopedMigrationStatus{}, err
			}
			if registrationOnly {
				result.Phase = "registration"
			}
			result.Available = operator.readiness(ctx) == nil
		}
		return result, nil
	}
	result.TaskID, result.Phase, result.AcceptedAt = state.Record.ID, string(
		state.Record.Phase,
	), &state.Record.AcceptedAt
	if state.Record.Phase == runnerscoped.Complete {
		result.CompletedAt = &state.Record.CompletedAt
	}
	return result, nil
}

type acceptedMigration struct {
	RunnerID   string `json:"runner_id"`
	PlanDigest string `json:"plan_digest"`
}

func (operator *Operator) StartScopedMigration(ctx context.Context,
	runnerID string, revision int64, body api.RunnerScopedMigrationRequest, key string,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil || revision <= 0 ||
		ids.Validate(githubconnection.IDKind, body.ConnectionID) != nil || body.RepositoryID <= 0 {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Runner migration requires an exact Runner revision and repository",
		)
	}
	current, err := operator.runners.GetRunner(ctx, runnerID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	desired := current.Record.Desired
	scopeKind := intent.ScopeKind(desired.OwnerKind)
	canonical := intent.CanonicalIntentV1{
		Method: http.MethodPost, Route: Route, Scope: intent.Scope{Kind: scopeKind, ID: desired.OwnerID},
		Path: []intent.PathBinding{{Name: "id", Value: runnerID}}, Query: intent.Object(),
		Body: intent.JSONBody(intent.Object(
			intent.Field{Name: "revision", Value: intent.String(strconv.FormatInt(revision, 10))},
			intent.Field{Name: "connection_id", Value: intent.String(body.ConnectionID)},
			intent.Field{Name: "repository_id", Value: intent.String(strconv.FormatInt(body.RepositoryID, 10))},
		)),
	}
	version, digest, err := intent.Canonicalize(ctx, canonical)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := operator.intents.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeKind(scopeKind),
		ScopeID: desired.OwnerID, Method: http.MethodPost, Route: Route, Key: key}
	if err := idempotency.ValidateIdempotencyLocator(locator); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if prior, found, err := operator.intents.ResolveExisting(ctx, operator.replays, locator, evidence); err != nil {
		return idempotency.IdempotencyResponse{}, err
	} else if found {
		return intent.CloneResponse(prior.Response), nil
	}
	if current.Revision != revision || current.Record.ProvisioningState != runners.RunnerProvisioningReady {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Runner is changed or has no completed registration",
		)
	}
	if err := operator.readiness(ctx); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	proof, err := operator.verification.VerifyRunnerRegistration(ctx, runnerID, body.ConnectionID, body.RepositoryID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	now := time.Now().UTC()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
		Action: workexecution.ActionUpdate, TargetID: runnerID, TargetName: desired.Slug,
		Scope: workexecution.Scope{
			TenantID:      desired.TenantID,
			ProjectID:     desired.ProjectID(),
			EnvironmentID: desired.EnvironmentID(),
		},
		CreatedAt: now, UpdatedAt: now, Deadline: now.Add(acceptedaction.QueueTimeout)}
	keyDigest := sha256.Sum256([]byte(key))
	record, change, err := operator.migration.PrepareBegin(
		ctx,
		runnerID,
		task.ID,
		revision,
		operator.policy,
		operator.candidateImage,
		proof.GitHubID,
		runnerscoped.Request{
			ConnectionID: body.ConnectionID,
			RepositoryID: body.RepositoryID,
			KeyDigest:    hex.EncodeToString(keyDigest[:]),
		},
		proof.Conditions,
	)
	if err != nil {
		if prior, found, replayErr := operator.intents.ResolveExisting(ctx, operator.replays, locator, evidence); replayErr != nil {
			return idempotency.IdempotencyResponse{}, replayErr
		} else if found {
			return intent.CloneResponse(prior.Response), nil
		}
		return idempotency.IdempotencyResponse{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	protected, err := evidence.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(protected.Ciphertext)
	return acceptedaction.Submit(ctx, operator.submitter, acceptedaction.Submission{
		Task: task, Kind: Kind, Locator: locator, Intent: protected, Evidence: evidence, Publication: change,
	}, acceptedMigration{RunnerID: runnerID, PlanDigest: record.Candidate.Digest()})
}
