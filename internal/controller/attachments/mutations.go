package attachments

import (
	"context"
	"crypto/rand"
	"errors"
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"io"
	"time"
)

const (
	attachCreationRoute        = "/attaches"
	attachDeletionRoute        = "/attaches/{id}"
	attachRenameRoute          = "/attaches/{id}/rename"
	attachMutationTimeout      = int64(120)
	maximumAttachMutationTries = 3
)

type attachMutationFacts interface {
	SealCustomHookBundle(
		context.Context,
		string,
		string,
		string,
		backinghook.Configuration,
	) ([]etcd.AttachFactSetMetadata, *etcd.AttachEncryptedFacts, *etcd.BackingHookEncryptedInputs, error)
	SealBackingHookTaskInputs(
		context.Context,
		string,
		string,
		backinghook.Configuration,
	) (*etcd.BackingHookEncryptedInputs, error)
	SealFactSets(
		context.Context,
		string,
		adapters.Adapter,
		adapters.Input,
		[]GrantInput,
	) ([]etcd.AttachFactSetMetadata, *etcd.AttachEncryptedFacts, error)
	ResolveReadyDatabase(
		context.Context,
		etcd.Versioned[etcd.AttachRecord],
		func(string) error,
	) error
	ResolveTaskIdentity(
		context.Context,
		etcd.Versioned[etcd.AttachRecord],
		string,
		controllerpkg.AttachPlanIdentityConsumer,
	) error
}

type attachDraftPlanSealer interface {
	SealDraft(
		context.Context,
		etcd.Versioned[etcd.AttachRecord],
		etcd.AttachTaskRenderInput,
		etcd.TaskRecord,
		*controllerpkg.AttachPlanIdentity,
		*etcd.AttachEncryptedFacts,
		*etcd.BackingHookEncryptedInputs,
	) (serviceruntimerecord.AttachPreparation, error)
}

type MutationService struct {
	repository  attachMutationRepository
	facts       attachMutationFacts
	plans       attachDraftPlanSealer
	runtime     attachRuntimeCapture
	idempotency attachMutationIdempotency
	random      io.Reader
	now         func() time.Time
}

func NewMutationService(
	repository attachMutationRepository,
	facts attachMutationFacts,
	plans attachDraftPlanSealer,
	runtime attachRuntimeCapture,
	idempotency attachMutationIdempotency,
) (*MutationService, error) {
	if repository == nil || facts == nil || plans == nil || runtime == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Attach mutation service is not configured")
	}
	return &MutationService{
		repository: repository, facts: facts, plans: plans, runtime: runtime, idempotency: idempotency,
		random: rand.Reader, now: time.Now,
	}, nil
}

func (service *MutationService) resolveMutationResult(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence attachMutationEvidence,
	result etcd.IdempotencyTransactionResult,
	mutationErr error,
	response etcd.IdempotencyResponse,
) (etcd.IdempotencyResponse, error) {
	var resolution requestidempotency.Resolution
	var err error
	if mutationErr != nil {
		if !isUnknownAttachMutationOutcome(mutationErr) {
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionReplay {
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	if resolution.Kind != requestidempotency.ResolutionApplied {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach mutation resolution is invalid")
	}
	return requestidempotency.CloneResponse(response), nil
}

func isUnknownAttachMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
