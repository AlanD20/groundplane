package entryfiles

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Resolver interface {
	ResolveTaskMaterializationSource(context.Context, string, taskmaterialization.Source) ([]byte, error)
}

// RemoveEntry edits only acknowledged file sources containing this Entry.
// Other values stay pinned to their applied generations, even if desired state
// already contains unrelated edits. No Compose consumer is selected for removal.
func RemoveEntry(ctx context.Context, entryID string, snapshot runtimeconfiguration.Snapshot,
	allocate func(ids.Kind, string) string, resolve Resolver,
) ([]taskmaterialization.Record, error) {
	if ids.Validate(ids.KindEnvEntry, entryID) != nil || allocate == nil || resolve == nil {
		return nil, errs.New(errs.KindValidationFailed, "Entry removal source identity is invalid")
	}
	var changes []taskmaterialization.Record
	for _, file := range taskmaterialization.Clone(snapshot.Files) {
		changed, remove := false, false
		switch file.Source.Kind {
		case taskmaterialization.SourceEntryValue:
			if file.Source.EntryValue == nil || file.Source.EntryValue.EntryID != entryID {
				continue
			}
			changed, remove = true, true
		case taskmaterialization.SourceGeneratedEnvironment:
			if file.Source.GeneratedEnvironment == nil {
				return nil, errs.New(errs.KindStateConflict, "acknowledged Entry environment file has no sources")
			}
			values := file.Source.GeneratedEnvironment.Values
			file.Source.GeneratedEnvironment.Values = slices.DeleteFunc(
				values,
				func(value taskmaterialization.GeneratedEnvironmentEntryReference) bool {
					return value.Value.EntryID == entryID
				},
			)
			changed = len(values) != len(file.Source.GeneratedEnvironment.Values)
			remove = file.ServiceID != "" && len(file.Source.GeneratedEnvironment.Values) == 0
		}
		if !changed {
			continue
		}
		if remove {
			switch file.OutputKind {
			case taskmaterialization.OutputGeneratedEnvironment:
				file.OutputKind = taskmaterialization.OutputRemoveGeneratedEnv
			case taskmaterialization.OutputPlainFile:
				file.OutputKind = taskmaterialization.OutputRemovePlainFile
			case taskmaterialization.OutputSecretFile:
				file.OutputKind = taskmaterialization.OutputRemoveSecretFile
			default:
				return nil, errs.New(errs.KindStateConflict, "acknowledged Entry source has an invalid output kind")
			}
			file.Source = taskmaterialization.Source{Kind: taskmaterialization.SourceRemoval}
		}
		file.StepID = allocate(ids.KindStep, "entry-removal-step/"+file.Destination)
		file.MaterializationID = allocate(ids.KindConfig, "entry-removal/"+file.Destination)
		var content []byte
		if !remove {
			var err error
			content, err = resolve.ResolveTaskMaterializationSource(ctx, snapshot.EnvironmentID, file.Source)
			if err != nil {
				clear(content)
				return nil, err
			}
		}
		digest := sha256.Sum256(content)
		file.Length, file.SHA256 = uint64(len(content)), hex.EncodeToString(digest[:])
		clear(content)
		changes = append(changes, file)
	}
	return changes, nil
}
