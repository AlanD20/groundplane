package attachments

import (
	"context"
	"crypto/rand"
	"errors"
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
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
	SealBackingHookInputsAtRevision(context.Context, string, string, backinghook.Configuration, int64) (*taskconfiguration.BackingHookEncryptedInputs, error)
	SealCustomHookBundle(
		context.Context,
		string,
		string,
		string,
		backinghook.Configuration,
	) ([]attachrecord.FactSetMetadata, *attachrecord.EncryptedFacts, *taskconfiguration.BackingHookEncryptedInputs, error)
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
}

type MutationService struct {
	repository  attachMutationRepository
	facts       attachMutationFacts
	idempotency attachMutationIdempotency
	random      io.Reader
	now         func() time.Time
}

func NewMutationService(
	repository attachMutationRepository,
	facts attachMutationFacts,
	idempotency attachMutationIdempotency,
) (*MutationService, error) {
	if repository == nil || facts == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Attach mutation service is not configured")
	}
	return &MutationService{
		repository: repository, facts: facts, idempotency: idempotency,
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
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Attach mutation resolution is invalid",
		)
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
