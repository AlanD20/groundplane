package runneridentity

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnercredential"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/core/runneraccess"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnergrants"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CapturedGrant binds explicit authority to the authenticated CLI credential.
// A mutation's image is controller-approved and immutable. DesiredRevision
// pins the approved Service declaration alongside its linked resources.
type CapturedGrant struct {
	Policy          runneraccess.Grant
	Image           string
	DesiredRevision int64
	Condition       keyvalue.Condition
	Task            keyvalue.Versioned[runnergrants.TaskProvenanceRecord]
	ExpiresAt       time.Time
}

// GrantReader reads the exact grant for an authenticated credential, action
// and target. A registration, repository selection, or Runner label is
// insufficient to grant an action.
type GrantReader interface {
	ReadRunnerGrant(context.Context, Principal, runneraccess.Operation, string) (CapturedGrant, error)
}

type credentialFence interface {
	AdmissionConditions(context.Context, [sha256.Size]byte, runnercredential.Binding) ([]keyvalue.Condition, error)
}

type nativeServiceDeploy interface {
	DeployServiceForRunnerGuarded(
		context.Context,
		string,
		release.ServiceDeployInput,
		string,
		[]keyvalue.Condition,
		runnergrants.TaskProvenance,
	) (idempotency.IdempotencyResponse, error)
}

type nativeServiceFetch interface {
	FetchServiceImageForRunnerGuarded(
		context.Context,
		string,
		string,
		string,
		[]string,
		[]keyvalue.Condition,
		workexecution.Scope,
		runnergrants.TaskProvenance,
	) (idempotency.IdempotencyResponse, error)
}

type ScopedAdmission struct {
	store       keyvalue.Store
	runners     *runners.Reader
	queue       *workqueue.Repository
	credentials credentialFence
	releases    nativeServiceDeploy
	images      nativeServiceFetch
	grants      GrantReader
}

func NewAdmission(
	store keyvalue.Store, registrations *runners.Reader, queue *workqueue.Repository,
	credentials credentialFence, releases nativeServiceDeploy, images nativeServiceFetch, grants GrantReader,
) (*ScopedAdmission, error) {
	if store == nil || registrations == nil || queue == nil || credentials == nil || releases == nil || images == nil {
		return nil, errs.New(errs.KindInternal, "Runner admission dependencies are required")
	}
	return &ScopedAdmission{
		store: store, runners: registrations, queue: queue, credentials: credentials, releases: releases,
		images: images, grants: grants,
	}, nil
}

func (admission *ScopedAdmission) Resolve(
	ctx context.Context,
	principal Principal,
	request Request,
) (ResolvedAction, error) {
	// Automatic GitHub deployment uses independently verified provider
	// evidence. That workflow does not rely on untrusted Runner reports.
	if principal.Credential.RequestID != "" || request.Operation == runneraccess.ReportBuild ||
		admission.grants == nil {
		return ResolvedAction{}, forbidden()
	}
	grant, err := admission.grants.ReadRunnerGrant(ctx, principal, request.Operation, request.ResourceID)
	if err != nil {
		return ResolvedAction{}, err
	}
	if grant.Policy.Operation != request.Operation || grant.Policy.Target.ID != request.ResourceID ||
		!grant.ExpiresAt.After(time.Now()) ||
		grant.Condition.Key == "" ||
		grant.Condition.ModRevision <= 0 ||
		grant.Condition.Prefix {
		return ResolvedAction{}, forbidden()
	}
	current, err := admission.runners.GetRunner(ctx, principal.Credential.RunnerID)
	if err != nil {
		return ResolvedAction{}, err
	}
	registration := runneraccess.Registration{
		RunnerID: current.Record.Desired.ID, TenantID: current.Record.Desired.TenantID,
		OwnerID: current.Record.Desired.OwnerID, OwnerKind: runner.OwnerKind(current.Record.Desired.OwnerKind),
	}
	expected := principal.Credential.RunnerEpoch
	if current.Record.RuntimeEpoch != expected || current.Record.ProvisioningState != runners.RunnerProvisioningReady {
		return ResolvedAction{}, forbidden()
	}
	guard := []keyvalue.Condition{
		grant.Condition, {Key: runners.RunnerKey(registration.RunnerID), ModRevision: current.Revision},
		{Key: runners.RunnerLifecycleKey(registration.RunnerID), ModRevision: current.Record.LifecycleRevision},
	}
	if request.IdempotencyKey != "" {
		request.IdempotencyKey = credentialIdempotencyKey(
			principal,
			grant.Condition.ModRevision,
			request.IdempotencyKey,
		)
	}
	policy, targetConditions, apply, err := admission.resolveAction(
		ctx,
		principal,
		request,
		grant,
		current.ReadRevision,
	)
	if err != nil {
		return ResolvedAction{}, err
	}
	guard = append(guard, targetConditions...)
	return ResolvedAction{
		Registration: registration, Grants: []runneraccess.Grant{grant.Policy}, Policy: policy, RunnerEpoch: expected,
		Apply: func(ctx context.Context) (Result, error) {
			credentialConditions, err := admission.credentials.AdmissionConditions(
				ctx,
				principal.Digest,
				principal.Credential,
			)
			if err != nil {
				return Result{}, err
			}
			conditions, err := mergeGuards(guard, credentialConditions)
			if err != nil {
				return Result{}, err
			}
			authorized, err := admission.queue.AuthorizeRead(ctx, conditions)
			if err != nil {
				return Result{}, err
			}
			now := time.Now()
			if !authorized || !principal.Credential.ActiveAt(now) || !grant.ExpiresAt.After(now) {
				return Result{}, forbidden()
			}
			return apply(ctx, conditions)
		},
	}, nil
}

type scopedAction func(context.Context, []keyvalue.Condition) (Result, error)

func credentialIdempotencyKey(principal Principal, grantRevision int64, key string) string {
	digest := hmac.New(sha256.New, principal.Digest[:])
	_, _ = digest.Write([]byte("groundplane.runner.request\x00" + strconv.FormatInt(grantRevision, 10) + "\x00" + key))
	return "runner:" + hex.EncodeToString(digest.Sum(nil))
}

func mergeGuards(base, more []keyvalue.Condition) ([]keyvalue.Condition, error) {
	conditions := make([]keyvalue.Condition, 0, len(base)+len(more))
	byKey := make(map[string]keyvalue.Condition, cap(conditions))
	for _, set := range [][]keyvalue.Condition{base, more} {
		for _, condition := range set {
			if existing, found := byKey[condition.Key]; found {
				if existing != condition {
					return nil, errs.New(errs.KindStateConflict, "Runner action authority changed")
				}
				continue
			}
			byKey[condition.Key] = condition
			conditions = append(conditions, condition)
		}
	}
	return conditions, nil
}
