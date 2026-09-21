package taskplanning

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	environmentfile "github.com/AlanD20/groundplane/internal/controller/environmentfile"
	"github.com/AlanD20/groundplane/internal/core"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"

	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
)

func entryRemovalMaterializationTemplates(
	intent environmentchanges.EntryRemovalIntent,
) ([]materializationrecord.Record, error) {
	if intent.CurrentProjection == nil || intent.CandidateProjection == nil ||
		intent.CurrentProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CandidateProjection.EnvironmentID != intent.EnvironmentID {
		return nil, errs.New(errs.KindInternal, "entry removal projection is unavailable")
	}
	var removed *entryrecord.Record
	for index := range intent.CurrentProjection.Entries {
		if intent.CurrentProjection.Entries[index].Entry.ID == intent.EntryID {
			value := intent.CurrentProjection.Entries[index]
			removed = &value
			break
		}
	}
	if removed == nil || removed.EnvironmentID != intent.EnvironmentID {
		return nil, errs.New(errs.KindInternal, "entry removal target is absent from its pinned projection")
	}
	for _, record := range intent.CandidateProjection.Entries {
		if record.Entry.ID == intent.EntryID {
			return nil, errs.New(errs.KindInternal, "entry removal candidate retained its target")
		}
	}
	if removed.Entry.Kind == core.EntryKindFile {
		if removed.Entry.UID == nil || removed.Entry.GID == nil {
			return nil, errs.New(errs.KindInternal, "file Entry removal metadata is incomplete")
		}
		kind := materializationrecord.OutputRemovePlainFile
		mode := entrymaterialization.ModeReadOnly
		if removed.Entry.Secret {
			kind = materializationrecord.OutputRemoveSecretFile
			mode = entrymaterialization.ModePrivate
		}
		return []materializationrecord.Record{{
			EnvironmentID: intent.EnvironmentID, Destination: removed.Entry.Path,
			OutputKind: kind, UID: *removed.Entry.UID, GID: *removed.Entry.GID, Mode: uint32(mode),
			SHA256: hex.EncodeToString(sha256.New().Sum(nil)),
			Source: materializationrecord.Source{Kind: materializationrecord.SourceRemoval},
		}}, nil
	}
	if removed.Entry.Kind != core.EntryKindEnv {
		return nil, errs.New(errs.KindInternal, "entry removal target kind is invalid")
	}
	scopes := append([]string(nil), removed.Entry.Exposure...)
	if removed.Entry.ExposesAll() {
		scopes = []string{"all"}
	} else {
		sort.Strings(scopes)
		for index, scope := range scopes {
			if scope == "all" || scope == "" || index > 0 && scope == scopes[index-1] {
				return nil, errs.New(errs.KindInternal, "entry removal exposure is invalid")
			}
		}
	}
	result := make([]materializationrecord.Record, len(scopes))
	for index, scope := range scopes {
		template, err := entryRemovalEnvironmentTemplate(intent, scope)
		if err != nil {
			return nil, err
		}
		result[index] = template
	}
	sort.Slice(result, func(left int, right int) bool {
		return result[left].Destination < result[right].Destination
	})
	return result, nil
}

func entryRemovalEnvironmentTemplate(
	intent environmentchanges.EntryRemovalIntent,
	scope string,
) (materializationrecord.Record, error) {
	reference := materializationrecord.Record{
		EnvironmentID: intent.EnvironmentID, OutputKind: materializationrecord.OutputGeneratedEnvironment,
		Mode: uint32(entrymaterialization.ModePrivate),
	}
	if scope == "all" {
		reference.Destination = environmentfile.EnvFileName(intent.EnvironmentID)
	} else {
		services, err := entryRemovalEnvironmentServiceIdentities(*intent.CandidateProjection)
		if err != nil {
			return materializationrecord.Record{}, err
		}
		for _, service := range services {
			if service.Name == scope {
				reference.ServiceID = service.ID
				reference.ServiceName = service.Name
				break
			}
		}
		if reference.ServiceID == "" {
			return materializationrecord.Record{}, errs.New(
				errs.KindInternal,
				"entry removal exposure Service is absent from its pinned projection",
			)
		}
		reference.Destination = environmentfile.ServiceEnvFileName(intent.EnvironmentID, scope)
	}
	values := make([]materializationrecord.GeneratedEnvironmentEntryReference, 0)
	for _, record := range intent.CandidateProjection.Entries {
		entry := record.Entry
		if entry.Kind != core.EntryKindEnv || scope == "all" && !entry.ExposesAll() ||
			scope != "all" && (entry.ExposesAll() || !entryExposesService(entry, scope)) {
			continue
		}
		storage := materializationrecord.EntryValueStoragePlain
		if entry.Secret {
			storage = materializationrecord.EntryValueStorageSecret
		}
		values = append(values, materializationrecord.GeneratedEnvironmentEntryReference{
			Name: entry.Key,
			Value: materializationrecord.EntryValueReference{
				EntryID: entry.ID, ValueGenerationID: record.CurrentValueGenerationID, Storage: storage,
			},
		})
	}
	sort.Slice(values, func(left int, right int) bool { return values[left].Name < values[right].Name })
	for index := 1; index < len(values); index++ {
		if values[index].Name == values[index-1].Name {
			return materializationrecord.Record{}, errs.New(
				errs.KindInternal,
				"entry removal generated Environment contains a duplicate key",
			)
		}
	}
	if scope != "all" && len(values) == 0 {
		reference.OutputKind = materializationrecord.OutputRemoveGeneratedEnv
		reference.SHA256 = hex.EncodeToString(sha256.New().Sum(nil))
		reference.Source = materializationrecord.Source{Kind: materializationrecord.SourceRemoval}
		return reference, nil
	}
	reference.Source = materializationrecord.Source{
		Kind: materializationrecord.SourceGeneratedEnvironment,
		GeneratedEnvironment: &materializationrecord.GeneratedEnvironmentValueReference{
			FormatVersion: 1,
			Values:        values,
		},
	}
	return reference, nil
}

func sameEntryRemovalMaterializationTemplate(
	reference materializationrecord.Record,
	template materializationrecord.Record,
) bool {
	if reference.EnvironmentID != template.EnvironmentID || reference.Destination != template.Destination ||
		reference.ServiceID != template.ServiceID || reference.ServiceName != template.ServiceName ||
		reference.OutputKind != template.OutputKind || reference.UID != template.UID || reference.GID != template.GID ||
		reference.Mode != template.Mode || !sameEntryRemovalMaterializationSource(reference.Source, template.Source) {
		return false
	}
	if entryRemovalOutputRemoves(reference.OutputKind) {
		emptyDigest := sha256.Sum256(nil)
		return reference.Length == 0 && reference.SHA256 == hex.EncodeToString(emptyDigest[:])
	}
	return true
}

func sameEntryRemovalMaterializationSource(
	left materializationrecord.Source,
	right materializationrecord.Source,
) bool {
	if left.Kind != right.Kind {
		return false
	}
	if left.Kind == materializationrecord.SourceRemoval {
		return left.BlueprintFile == nil && left.ComponentFile == nil && left.EntryValue == nil &&
			left.GeneratedEnvironment == nil && right.BlueprintFile == nil && right.ComponentFile == nil &&
			right.EntryValue == nil && right.GeneratedEnvironment == nil
	}
	if left.GeneratedEnvironment == nil || right.GeneratedEnvironment == nil ||
		left.GeneratedEnvironment.FormatVersion != right.GeneratedEnvironment.FormatVersion ||
		len(left.GeneratedEnvironment.Values) != len(right.GeneratedEnvironment.Values) {
		return false
	}
	for index := range left.GeneratedEnvironment.Values {
		if left.GeneratedEnvironment.Values[index] != right.GeneratedEnvironment.Values[index] {
			return false
		}
	}
	return true
}

func entryRemovalOutputRemoves(kind materializationrecord.OutputKind) bool {
	return kind == materializationrecord.OutputRemoveGeneratedEnv ||
		kind == materializationrecord.OutputRemovePlainFile ||
		kind == materializationrecord.OutputRemoveSecretFile
}
