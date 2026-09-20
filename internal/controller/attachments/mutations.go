package attachments

import (
	"context"
	"crypto/rand"
	"errors"
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
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
	) ([]attachrecord.FactSetMetadata, *attachrecord.EncryptedFacts, *etcd.BackingHookEncryptedInputs, error)
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
	) ([]attachrecord.FactSetMetadata, *attachrecord.EncryptedFacts, error)
	ResolveReadyDatabase(
		context.Context,
		etcdstore.Versioned[attachrecord.Record],
		func(string) error,
	) error
	ResolveTaskIdentity(
		context.Context,
		etcdstore.Versioned[attachrecord.Record],
		string,
		taskplanning.AttachPlanIdentityConsumer,
	) error
}

type attachDraftPlanSealer interface {
	SealDraft(
		context.Context,
		etcdstore.Versioned[attachrecord.Record],
		etcd.AttachTaskRenderInput,
		etcd.TaskRecord,
		*taskplanning.AttachPlanIdentity,
		*attachrecord.EncryptedFacts,
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
	locator idempotencyrecord.IdempotencyLocator,
	evidence attachMutationEvidence,
	result etcd.IdempotencyTransactionResult,
	mutationErr error,
	response idempotencyrecord.IdempotencyResponse,
) (idempotencyrecord.IdempotencyResponse, error) {
	var resolution requestidempotency.Resolution
	var err error
	if mutationErr != nil {
		if !isUnknownAttachMutationOutcome(mutationErr) {
			return idempotencyrecord.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionReplay {
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	if resolution.Kind != requestidempotency.ResolutionApplied {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Attach mutation resolution is invalid")
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
