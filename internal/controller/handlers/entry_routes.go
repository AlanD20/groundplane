package handlers

import (
	"context"
	entrycapability "github.com/AlanD20/groundplane/internal/controller/entry"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/danielgtaylor/huma/v2"
	"net/http"
	"reflect"
	"strconv"
)

type EntryReader interface {
	GetEntry(context.Context, string) (etcdstore.Versioned[entryrecord.Record], error)
	ListEntries(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[entryrecord.Record], error)
	SecretValueEmpty(context.Context, entryrecord.Record) (bool, error)
	RevealEntry(context.Context, string) (string, error)
}

type EntryMutator interface {
	CreateEntry(
		context.Context,
		apiTypes.EntryCreateRequest,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	BulkUpsertEntries(
		context.Context,
		apiTypes.EntryBulkUpsertRequest,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	EditEntry(
		context.Context,
		string,
		apiTypes.EntryEditRequest,
		string,
	) (idempotencyrecord.IdempotencyResponse, error)
	RemoveEntry(context.Context, entrycapability.RemoveRequest) (entrycapability.RemovalOutcome, error)
}

type entryCreateInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type entryEditInput struct {
	ID             string `path:"id" pattern:"^ev_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type entryBulkUpsertInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	RawBody        []byte
}

type entryRemoveInput struct {
	ID             string `path:"id" pattern:"^ev_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
}

type entryListInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit       int    `query:"limit"       required:"false"`
	Cursor      string `query:"cursor"      required:"false"`
}

type entryShowInput struct {
	ID string `path:"id" pattern:"^ev_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type entryPageOutput struct {
	Body apiTypes.Page[apiTypes.Entry]
}

type entryOutput struct {
	Body apiTypes.Entry
}

type entryValueOutput struct {
	Body apiTypes.EntryValue
}

type entryMutationOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

type entryRemoveOutput struct {
	Body apiTypes.TaskAccepted
}

func (s *Server) registerEntries() {
	entrySchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.Entry](),
		true,
		"Entry",
	)
	taskAcceptedSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.TaskAccepted](),
		true,
		"TaskAccepted",
	)
	operation := huma.Operation{
		OperationID: "entry.create", Method: http.MethodPost, Path: "/entries",
		Summary: "Create an environment Entry", Tags: []string{"Entry"},
		DefaultStatus: http.StatusCreated, SkipValidateBody: true,
		Middlewares: huma.Middlewares{s.rejectEntryQuery},
	}
	operation.RequestBody = &huma.RequestBody{
		Required: true,
		Content: map[string]*huma.MediaType{
			"application/json": {
				Schema: s.API.OpenAPI().Components.Schemas.Schema(
					reflect.TypeFor[apiTypes.EntryCreateRequest](),
					true,
					"EntryCreateRequest",
				),
			},
		},
	}
	operation.Responses = map[string]*huma.Response{
		strconv.Itoa(http.StatusCreated): {
			Description: http.StatusText(http.StatusCreated),
			Content: map[string]*huma.MediaType{
				"application/json": {Schema: entrySchema},
			},
		},
	}
	huma.Register(s.API, operation, s.createEntry)
	bulkResponseSchema := s.API.OpenAPI().Components.Schemas.Schema(
		reflect.TypeFor[apiTypes.EntryBulkUpsertResult](), true, "EntryBulkUpsertResult",
	)
	bulkOperation := huma.Operation{
		OperationID: "entry.bulk-upsert", Method: http.MethodPost, Path: "/entries/bulk",
		Summary: "Atomically upsert literal environment variables", Tags: []string{"Entry"},
		DefaultStatus: http.StatusAccepted, SkipValidateBody: true,
		Middlewares: huma.Middlewares{s.rejectEntryQuery},
	}
	bulkOperation.RequestBody = &huma.RequestBody{
		Required: true,
		Content: map[string]*huma.MediaType{
			"application/json": {
				Schema: s.API.OpenAPI().Components.Schemas.Schema(
					reflect.TypeFor[apiTypes.EntryBulkUpsertRequest](), true, "EntryBulkUpsertRequest",
				),
			},
		},
	}
	bulkOperation.Responses = map[string]*huma.Response{
		strconv.Itoa(http.StatusAccepted): {
			Description: http.StatusText(http.StatusAccepted),
			Content: map[string]*huma.MediaType{
				"application/json": {Schema: bulkResponseSchema},
			},
		},
	}
	huma.Register(s.API, bulkOperation, s.bulkUpsertEntries)
	editOperation := huma.Operation{
		OperationID: "entry.edit", Method: http.MethodPatch, Path: "/entries/{id}",
		Summary: "Edit an environment Entry's source and exposure", Tags: []string{"Entry"},
		DefaultStatus: http.StatusOK, SkipValidateBody: true,
		Middlewares: huma.Middlewares{s.rejectEntryQuery},
	}
	editOperation.RequestBody = &huma.RequestBody{
		Required: true,
		Content: map[string]*huma.MediaType{
			"application/json": {
				Schema: s.API.OpenAPI().Components.Schemas.Schema(
					reflect.TypeFor[apiTypes.EntryEditRequest](), true, "EntryEditRequest",
				),
			},
		},
	}
	editOperation.Responses = map[string]*huma.Response{
		strconv.Itoa(http.StatusOK): {
			Description: http.StatusText(http.StatusOK),
			Content: map[string]*huma.MediaType{
				"application/json": {Schema: entrySchema},
			},
		},
	}
	huma.Register(s.API, editOperation, s.editEntry)
	huma.Register(s.API, huma.Operation{
		OperationID: "entry.remove", Method: http.MethodDelete, Path: "/entries/{id}",
		Summary: "Remove an environment Entry", Tags: []string{"Entry"},
		DefaultStatus: http.StatusAccepted,
		Middlewares:   huma.Middlewares{s.rejectEntryDeleteBody, s.rejectEntryQuery},
		Responses:     attachMutationResponses(taskAcceptedSchema),
	}, s.removeEntry)
	huma.Register(s.API, huma.Operation{
		OperationID: "entry.list", Method: http.MethodGet, Path: "/entries",
		Summary: "List environment entries", Tags: []string{"Entry"},
		Middlewares: huma.Middlewares{s.validateEntryListQuery},
	}, s.listEntries)
	huma.Register(s.API, huma.Operation{
		OperationID: "entry.show", Method: http.MethodGet, Path: "/entries/{id}",
		Summary: "Show environment Entry metadata", Tags: []string{"Entry"},
	}, s.showEntry)
	huma.Register(s.API, huma.Operation{
		OperationID: "entry.reveal", Method: http.MethodGet, Path: "/entries/{id}/value",
		Summary: "Reveal an encrypted Entry value", Tags: []string{"Entry"},
	}, s.revealEntry)
	s.setRoutePolicy("POST /api/v1/entries", routePolicy{body: jsonBody})
	s.setRoutePolicy("POST /api/v1/entries/bulk", routePolicy{body: jsonBody})
	s.setRoutePolicy("PATCH /api/v1/entries/{id}", routePolicy{body: jsonBody})
}
