package attachments

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type attachFactRecordReader interface {
	GetAttach(context.Context, string) (etcd.Versioned[attachrecord.Record], error)
}

type attachFactValueResolver interface {
	ResolveFact(context.Context, string, core.FactRef, bool, secretvalue.PlaintextConsumer) error
}

type attachFactReadService struct {
	records attachFactRecordReader
	facts   attachFactValueResolver
}

func NewFactReadService(
	records attachFactRecordReader,
	facts attachFactValueResolver,
) (*attachFactReadService, error) {
	if records == nil || facts == nil {
		return nil, errs.New(errs.KindInternal, "Attach fact reads are not configured")
	}
	return &attachFactReadService{records: records, facts: facts}, nil
}

func (service *attachFactReadService) RevealAttachFact(
	ctx context.Context,
	attachID string,
	grantAttachID string,
	key string,
) (string, error) {
	if ctx == nil {
		return "", errs.New(errs.KindInternal, "Attach fact read context is required")
	}
	current, err := service.records.GetAttach(ctx, attachID)
	if err != nil {
		return "", err
	}
	value := ""
	err = service.facts.ResolveFact(
		ctx,
		current.Record.EnvironmentID,
		core.FactRef{Attach: current.Record.ID, Grant: grantAttachID, Key: key},
		true,
		func(plaintext []byte) error {
			if !utf8.Valid(plaintext) {
				return errs.New(errs.KindInternal, "Attach fact is not valid UTF-8")
			}
			value = string(plaintext)
			return nil
		},
	)
	if err != nil {
		return "", err
	}
	return value, nil
}
