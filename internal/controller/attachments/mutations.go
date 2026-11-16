package attachments

import (
	"context"
	"crypto/rand"
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
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
	SealBackingHookInputsAtRevision(
		context.Context,
		string,
		string,
		backinghook.Configuration,
		int64,
	) (*taskconfiguration.BackingHookEncryptedInputs, error)
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
	accepted    *acceptedaction.Submitter
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
	accepted *acceptedaction.Submitter,
) (*MutationService, error) {
	if repository == nil || facts == nil || idempotency == nil || accepted == nil {
		return nil, errs.New(errs.KindInternal, "Attach mutation service is not configured")
	}
	return &MutationService{
		repository: repository, facts: facts, idempotency: idempotency, accepted: accepted,
		random: rand.Reader, now: time.Now,
	}, nil
}
