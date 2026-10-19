package operations

import (
	"context"
	"errors"
	"sort"
	"time"

	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	entryCreationRoute           = "/entries"
	maximumEntryCreationAttempts = 3
	maximumEntryNumericID        = int64(1<<32 - 2)
)

type entryCreationGenerator interface {
	Generate(
		context.Context,
		string,
		string,
		core.EnvEntry,
		string,
		time.Time,
	) (entryrecord.EntryValueGeneration, error)
}

func prepareEntryCreation(input apiTypes.EntryCreateRequest) (core.EnvEntry, error) {
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil {
		return core.EnvEntry{}, errs.New(errs.KindValidationFailed, "Entry creation requires a stable Environment id")
	}
	source, err := entryCreationSource(input.Source)
	if err != nil {
		return core.EnvEntry{}, err
	}
	exposure, err := normalizeEntryExposure(input.Exposure)
	if err != nil {
		return core.EnvEntry{}, err
	}
	uid, err := entryCreationNumericID(input.UID, "uid")
	if err != nil {
		return core.EnvEntry{}, err
	}
	gid, err := entryCreationNumericID(input.GID, "gid")
	if err != nil {
		return core.EnvEntry{}, err
	}
	entry := core.EnvEntry{
		ID: ids.New(ids.KindEnvEntry), Kind: core.EntryKind(input.Type), Key: input.Key, Path: input.Path,
		UID: uid, GID: gid, Source: source, Exposure: exposure, Secret: input.Secret,
	}
	if len(entry.Source.Literal) > apiTypes.MaximumEntryValueBytes {
		return core.EnvEntry{}, errs.New(errs.KindValidationFailed, "Entry literal exceeds the 256 KiB limit")
	}
	if entry.Source.Kind == core.SourceSecretRef && !entry.Secret {
		return core.EnvEntry{}, errs.New(errs.KindValidationFailed, "Reusable Secret requires a secret Entry")
	}
	validationEntry := entry
	if validationEntry.Secret && validationEntry.Source.Kind == core.SourceLiteral {
		validationEntry.Source.Literal = ""
	}
	if err := validationEntry.Validate(); err != nil {
		return core.EnvEntry{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	if entry.Kind == core.EntryKindFile {
		if err := entrymaterialization.ValidateDesiredDestination(entry.Path); err != nil {
			return core.EnvEntry{}, err
		}
	}
	return entry, nil
}

func entryCreationSource(input apiTypes.EntrySource) (core.EntrySource, error) {
	source := core.EntrySource{
		Kind: core.EntrySourceKind(input.Kind), Literal: input.Literal, SecretRef: input.SecretRef,
	}
	switch source.Kind {
	case core.SourceLiteral:
		if input.SecretRef != "" || input.AttachID != "" || input.GrantAttachID != "" || input.Fact != "" {
			return core.EntrySource{}, errs.New(
				errs.KindValidationFailed,
				"Entry literal source carries another source kind",
			)
		}
	case core.SourceSecretRef:
		if input.SecretRef == "" || input.Literal != "" || input.AttachID != "" ||
			input.GrantAttachID != "" || input.Fact != "" {
			return core.EntrySource{}, errs.New(errs.KindValidationFailed, "Entry secret_ref source is invalid")
		}
	case core.SourceFact:
		if input.Literal != "" || input.SecretRef != "" || input.AttachID == "" || input.Fact == "" ||
			ids.Validate(ids.KindAttach, input.AttachID) != nil ||
			(input.GrantAttachID != "" && ids.Validate(ids.KindAttach, input.GrantAttachID) != nil) {
			return core.EntrySource{}, errs.New(errs.KindValidationFailed, "Entry fact source is invalid")
		}
		source.Fact = &core.FactRef{Attach: input.AttachID, Grant: input.GrantAttachID, Key: input.Fact}
	default:
		return core.EntrySource{}, errs.New(errs.KindValidationFailed, "Entry source kind is invalid")
	}
	return source, nil
}

func normalizeEntryExposure(input []string) ([]string, error) {
	if len(input) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Entry exposure is required")
	}
	exposure := append([]string(nil), input...)
	seen := make(map[string]struct{}, len(exposure))
	for _, value := range exposure {
		if value == "" {
			return nil, errs.New(errs.KindValidationFailed, "Entry exposure contains an empty service")
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "Entry exposure contains a duplicate service")
		}
		seen[value] = struct{}{}
	}
	if _, all := seen["all"]; all {
		if len(exposure) != 1 {
			return nil, errs.New(errs.KindValidationFailed, "Entry exposure all must be the only value")
		}
		return []string{"all"}, nil
	}
	sort.Strings(exposure)
	return exposure, nil
}

func entryCreationNumericID(value *int64, field string) (*uint32, error) {
	if value == nil {
		return nil, nil
	}
	if *value < 0 || *value > maximumEntryNumericID {
		return nil, errs.Newf(errs.KindValidationFailed, "Entry %s is outside the supported numeric range", field)
	}
	converted := uint32(*value)
	return &converted, nil
}

func entryCreationResponse(entry core.EnvEntry) apiTypes.Entry {
	response := apiTypes.Entry{
		ID: entry.ID, Type: string(entry.Kind), Key: entry.Key, Path: entry.Path,
		Source: apiTypes.EntrySource{
			Kind: string(entry.Source.Kind), Literal: entry.Source.Literal, SecretRef: entry.Source.SecretRef,
		},
		Exposure: append([]string(nil), entry.Exposure...), Secret: entry.Secret,
	}
	if entry.UID != nil {
		value := int64(*entry.UID)
		response.UID = &value
	}
	if entry.GID != nil {
		value := int64(*entry.GID)
		response.GID = &value
	}
	if entry.Source.Fact != nil {
		response.Source.AttachID = entry.Source.Fact.Attach
		response.Source.GrantAttachID = entry.Source.Fact.Grant
		response.Source.Fact = entry.Source.Fact.Key
	}
	return response
}

func isUnknownEntryCreationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
