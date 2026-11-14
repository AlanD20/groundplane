package runtimeconfiguration

import "github.com/AlanD20/groundplane/internal/common/taskmaterialization"

// ReferencesEntry consults acknowledged sources, never the presence of a
// desired Entry or an unrelated running Service.
func (snapshot Snapshot) ReferencesEntry(entryID string) bool {
	for _, file := range snapshot.Files {
		switch file.Source.Kind {
		case taskmaterialization.SourceEntryValue:
			if file.Source.EntryValue != nil && file.Source.EntryValue.EntryID == entryID {
				return true
			}
		case taskmaterialization.SourceGeneratedEnvironment:
			if file.Source.GeneratedEnvironment == nil {
				continue
			}
			for _, value := range file.Source.GeneratedEnvironment.Values {
				if value.Value.EntryID == entryID {
					return true
				}
			}
		}
	}
	return false
}
