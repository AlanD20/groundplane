package operations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

type acceptedBulkEntry struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type acceptedEntryBulkUpsert struct {
	Bindings      etcd.EntryMutationBindings        `json:"bindings"`
	EnvironmentID string                            `json:"environment_id"`
	Entries       []acceptedBulkEntry               `json:"entries"`
	Exposure      []string                          `json:"exposure"`
	Secret        bool                              `json:"secret"`
	Encryption    string                            `json:"encryption,omitempty"`
	After         []string                          `json:"after,omitempty"`
	Locator       idempotency.IdempotencyLocator    `json:"locator"`
	Intent        idempotency.ProtectedIntentRecord `json:"intent"`
}

func (request acceptedEntryBulkUpsert) input() (entryBulkUpsertInput, error) {
	entries := make([]apiTypes.EntryBulkItem, len(request.Entries))
	for i, item := range request.Entries {
		entries[i] = apiTypes.EntryBulkItem{Key: item.Key, Value: string(item.Value)}
	}
	return prepareEntryBulkUpsert(
		apiTypes.EntryBulkUpsertRequest{
			EnvironmentID: request.EnvironmentID,
			Entries:       entries,
			Exposure:      request.Exposure,
			Secret:        request.Secret,
			Encryption:    request.Encryption,
			After:         request.After,
		},
	)
}

func (service *entryBulkUpsertService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.accepted,
			"entry-bulk",
			workexecution.ActionUpdate,
			ids.KindEnvironment,
			service.prepareAcceptedBulk,
		),
	}
}

func (service *entryBulkUpsertService) BulkUpsertEntries(
	ctx context.Context,
	request apiTypes.EntryBulkUpsertRequest,
	key string,
) (idempotency.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry bulk upsert context is required")
	}
	input, err := prepareEntryBulkUpsert(request)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	locator := idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID:   input.environmentID,
		Method:    http.MethodPost,
		Route:     entryBulkUpsertRoute,
		Key:       key,
	}
	intent, err := service.accepted.Begin(ctx, bulkCanonicalIntent(input), locator)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer intent.Destroy()
	if replay, ok := intent.Replay(); ok {
		return replay, nil
	}
	environment, err := service.desired.repository.GetEnvironment(ctx, input.environmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	project, err := service.desired.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	bindings, err := service.desired.work.BindEntryMutationReferences(
		ctx,
		input.environmentID,
		project.Record.ID,
		"",
		input.exposure,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	at := service.desired.now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionUpdate,
		TargetID:   input.environmentID,
		TargetName: "Entries",
		Scope: workexecution.Scope{
			TenantID:      project.Record.TenantID,
			ProjectID:     project.Record.ID,
			EnvironmentID: input.environmentID,
		},
		After: append(
			[]string(nil),
			input.after...),
		CreatedAt: at,
		UpdatedAt: at,
		Deadline:  at.Add(acceptedaction.QueueTimeout),
	}
	entries := make([]acceptedBulkEntry, len(input.entries))
	for i, item := range input.entries {
		entries[i] = acceptedBulkEntry{Key: item.key, Value: []byte(item.value)}
	}
	defer func() {
		for _, item := range entries {
			clear(item.Value)
		}
	}()
	payload := acceptedEntryBulkUpsert{
		Bindings:      bindings,
		EnvironmentID: input.environmentID,
		Entries:       entries,
		Exposure:      input.exposure,
		Secret:        input.secret,
		Encryption:    input.encryption,
		After:         input.after,
		Locator:       locator,
		Intent:        intent.Record(),
	}
	defer clear(payload.Intent.Ciphertext)
	return acceptedaction.Submit(ctx, service.accepted, intent.Submission(task, "entry-bulk"), payload)
}
