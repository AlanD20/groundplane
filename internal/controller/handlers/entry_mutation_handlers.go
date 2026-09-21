package handlers

import (
	"context"
	entrycapability "github.com/AlanD20/groundplane/internal/controller/entry"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
	"log/slog"
)

func (s *Server) bulkUpsertEntries(
	ctx context.Context,
	request *entryBulkUpsertInput,
) (*entryMutationOutput, error) {
	if s.entryMutations == nil {
		return nil, errs.New(errs.KindInternal, "entry mutator is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeEntryBulkUpsert(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.entryMutations.BulkUpsertEntries(ctx, input, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &entryMutationOutput{
		Status:      response.Status,
		ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, writeErr := ctx.BodyWriter().Write(response.Body); writeErr != nil && s.Logger != nil {
				s.Logger.Error("controller: write Entry bulk upsert response", slog.Any("error", writeErr))
			}
		},
	}, nil
}

func (s *Server) createEntry(
	ctx context.Context,
	request *entryCreateInput,
) (*entryMutationOutput, error) {
	if s.entryMutations == nil {
		return nil, errs.New(errs.KindInternal, "entry mutator is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeEntryCreate(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.entryMutations.CreateEntry(ctx, input, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &entryMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, writeErr := ctx.BodyWriter().Write(response.Body); writeErr != nil && s.Logger != nil {
				s.Logger.Error("controller: write Entry creation response", slog.Any("error", writeErr))
			}
		},
	}, nil
}

func (s *Server) editEntry(
	ctx context.Context,
	request *entryEditInput,
) (*entryMutationOutput, error) {
	if s.entryMutations == nil {
		return nil, errs.New(errs.KindInternal, "entry mutator is not configured")
	}
	defer clear(request.RawBody)
	input, err := decodeEntryEdit(request.RawBody)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response, err := s.entryMutations.EditEntry(ctx, request.ID, input, request.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &entryMutationOutput{
		Status: response.Status, ContentType: response.ContentKind,
		Body: func(ctx huma.Context) {
			ctx.SetStatus(response.Status)
			if _, writeErr := ctx.BodyWriter().Write(response.Body); writeErr != nil && s.Logger != nil {
				s.Logger.Error("controller: write Entry edit response", slog.Any("error", writeErr))
			}
		},
	}, nil
}

func (s *Server) removeEntry(
	ctx context.Context,
	request *entryRemoveInput,
) (*entryRemoveOutput, error) {
	if s.entryMutations == nil {
		return nil, errs.New(errs.KindInternal, "entry mutator is not configured")
	}
	outcome, err := s.entryMutations.RemoveEntry(ctx, entrycapability.RemoveRequest{
		EntryID: request.ID, IdempotencyKey: request.IdempotencyKey,
	})
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &entryRemoveOutput{Body: apiTypes.TaskAccepted{TaskID: outcome.TaskID}}, nil
}
