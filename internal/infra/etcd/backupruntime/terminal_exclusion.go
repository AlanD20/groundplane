package backupruntime

import (
	"slices"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// TerminalExclusionKeys returns the stable unique source-target exclusions
// retained by a terminal Backup receipt. Config sources have no exclusion key.
func TerminalExclusionKeys(sources []BackupTerminalSourceOutcome) ([]string, error) {
	byKey := make(map[string]struct{})
	for _, source := range sources {
		var kind BackupSourceTargetKind
		switch source.Kind {
		case BackupRuntimeSourceAttach:
			kind = BackupSourceTargetAttach
		case BackupRuntimeSourceVolume:
			kind = BackupSourceTargetVolume
		case BackupRuntimeSourceConfig:
			continue
		default:
			return nil, errs.New(errs.KindInternal, "backup terminal source kind is invalid")
		}
		key, err := BackupSourceTargetExclusionKey(kind, source.TargetID)
		if err != nil {
			return nil, err
		}
		byKey[key] = struct{}{}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys, nil
}
