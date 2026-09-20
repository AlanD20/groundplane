package operations

import (
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
	"sort"
)

type entryBulkPair struct {
	key   string
	value string
}

type entryBulkUpsertInput struct {
	environmentID string
	entries       []entryBulkPair
	exposure      []string
	secret        bool
}

func prepareEntryBulkUpsert(request apiTypes.EntryBulkUpsertRequest) (entryBulkUpsertInput, error) {
	if ids.Validate(ids.KindEnvironment, request.EnvironmentID) != nil {
		return entryBulkUpsertInput{}, errs.New(
			errs.KindValidationFailed,
			"Entry bulk upsert requires a stable Environment id",
		)
	}
	if len(request.Entries) == 0 || len(request.Entries) > apiTypes.MaximumBulkEntryCount {
		return entryBulkUpsertInput{}, errs.Newf(
			errs.KindValidationFailed,
			"Entry bulk upsert requires 1 through %d entries",
			apiTypes.MaximumBulkEntryCount,
		)
	}
	exposure, err := normalizeEntryExposure(request.Exposure)
	if err != nil {
		return entryBulkUpsertInput{}, err
	}
	seen := make(map[string]struct{}, len(request.Entries))
	entries := make([]entryBulkPair, len(request.Entries))
	totalBytes := 0
	for index, item := range request.Entries {
		if _, duplicate := seen[item.Key]; duplicate {
			return entryBulkUpsertInput{}, errs.Newf(errs.KindValidationFailed, "Entry key %q is duplicated", item.Key)
		}
		seen[item.Key] = struct{}{}
		if len(item.Value) > apiTypes.MaximumEntryValueBytes {
			return entryBulkUpsertInput{}, errs.Newf(
				errs.KindValidationFailed,
				"Entry %q literal exceeds the 256 KiB limit",
				item.Key,
			)
		}
		totalBytes += len(item.Key) + len(item.Value)
		if totalBytes > apiTypes.MaximumBulkEntryPayloadBytes {
			return entryBulkUpsertInput{}, errs.New(
				errs.KindValidationFailed,
				"Entry bulk upsert exceeds the 1 MiB value limit",
			)
		}
		validation := core.EnvEntry{
			ID: ids.New(ids.KindEnvEntry), Kind: core.EntryKindEnv, Key: item.Key,
			Source:   core.EntrySource{Kind: core.SourceLiteral, Literal: item.Value},
			Exposure: append([]string(nil), exposure...), Secret: request.Secret,
		}
		if validation.Secret {
			validation.Source.Literal = ""
		}
		if err := validation.Validate(); err != nil {
			return entryBulkUpsertInput{}, errs.Wrap(errs.KindValidationFailed, err)
		}
		entries[index] = entryBulkPair{key: item.Key, value: item.Value}
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].key < entries[right].key })
	return entryBulkUpsertInput{
		environmentID: request.EnvironmentID,
		entries:       entries,
		exposure:      exposure,
		secret:        request.Secret,
	}, nil
}

func entryBulkUpsertResponse(changes []entryBulkChange, taskID string) (etcd.IdempotencyResponse, error) {
	entries := make([]apiTypes.Entry, len(changes))
	for index, change := range changes {
		entries[index] = entryCreationResponse(change.record.Entry)
	}
	body, err := json.Marshal(apiTypes.EntryBulkUpsertResult{TaskID: taskID, Entries: entries})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return etcd.IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: body}, nil
}
