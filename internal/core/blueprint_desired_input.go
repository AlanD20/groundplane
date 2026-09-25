package core

// BlueprintDesiredInput is the normalized, replayable operator input for an
// Environment revision. It contains no rendered artifact, observed runtime,
// generated path, or secret plaintext. A coordinator must not reparse the
// accepted audit bundle to recover these decisions after a restart.
type BlueprintDesiredInput struct {
	NormalizedCompose []byte                          `json:"normalized_compose"`
	RuntimeFiles      []BlueprintFile                 `json:"runtime_files,omitempty"`
	ServiceExtensions map[string]ServiceExtensionSpec `json:"service_extensions,omitempty"`
	NetworkPool       string                          `json:"network_pool"`
	Requires          []Requirement                   `json:"requires,omitempty"`
	Attachments       map[string]AttachmentSpec       `json:"attachments,omitempty"`
	Entries           map[string]EntrySpec            `json:"entries,omitempty"`
	Routes            []RouteSpec                     `json:"routes,omitempty"`
	Scripts           map[string]ScriptSpec           `json:"scripts,omitempty"`
	Components        map[string]ComponentSpec        `json:"components,omitempty"`
	Backup            *BackupSpec                     `json:"backup,omitempty"`
	ReleaseGroups     map[string]ReleaseGroupSpec     `json:"release_groups,omitempty"`
}

// CloneBlueprintDesiredInput returns a detached copy suitable for immutable
// ownership by a durable revision or an execution plan.
func CloneBlueprintDesiredInput(source BlueprintDesiredInput) BlueprintDesiredInput {
	clone := source
	clone.NormalizedCompose = append([]byte(nil), source.NormalizedCompose...)
	if source.RuntimeFiles != nil {
		clone.RuntimeFiles = make([]BlueprintFile, len(source.RuntimeFiles))
		for index, file := range source.RuntimeFiles {
			clone.RuntimeFiles[index] = BlueprintFile{
				Path: file.Path, Content: append([]byte(nil), file.Content...),
			}
		}
	}
	if source.ServiceExtensions != nil {
		clone.ServiceExtensions = make(map[string]ServiceExtensionSpec, len(source.ServiceExtensions))
		for service, extension := range source.ServiceExtensions {
			clone.ServiceExtensions[service] = CloneServiceExtension(extension)
		}
	}
	if source.Requires != nil {
		clone.Requires = make([]Requirement, len(source.Requires))
		for index, requirement := range source.Requires {
			clone.Requires[index] = requirement
			clone.Requires[index].Phases = append([]RequirementPhase(nil), requirement.Phases...)
		}
	}
	if source.Attachments != nil {
		clone.Attachments = make(map[string]AttachmentSpec, len(source.Attachments))
		for name, attachment := range source.Attachments {
			attachment.Grants = append([]string(nil), attachment.Grants...)
			clone.Attachments[name] = attachment
		}
	}
	if source.Entries != nil {
		clone.Entries = make(map[string]EntrySpec, len(source.Entries))
		for name, entry := range source.Entries {
			entry.Exposure = append([]string(nil), entry.Exposure...)
			if entry.Source.Fact != nil {
				fact := *entry.Source.Fact
				entry.Source.Fact = &fact
			}
			if entry.UID != nil {
				uid := *entry.UID
				entry.UID = &uid
			}
			if entry.GID != nil {
				gid := *entry.GID
				entry.GID = &gid
			}
			clone.Entries[name] = entry
		}
	}
	clone.Routes = append([]RouteSpec(nil), source.Routes...)
	if source.Scripts != nil {
		clone.Scripts = make(map[string]ScriptSpec, len(source.Scripts))
		for key, script := range source.Scripts {
			if script.Execution != nil {
				execution := *script.Execution
				execution.Volumes = make([]ScriptVolumeGrantSpec, len(script.Execution.Volumes))
				for index, grant := range script.Execution.Volumes {
					execution.Volumes[index] = grant
					if grant.ReadOnly != nil {
						readOnly := *grant.ReadOnly
						execution.Volumes[index].ReadOnly = &readOnly
					}
				}
				execution.Entries = append([]string(nil), script.Execution.Entries...)
				script.Execution = &execution
			}
			clone.Scripts[key] = script
		}
	}
	if source.Components != nil {
		clone.Components = make(map[string]ComponentSpec, len(source.Components))
		for capability, component := range source.Components {
			component.Settings.ZoneIDs = append([]string(nil), component.Settings.ZoneIDs...)
			clone.Components[capability] = component
		}
	}
	if source.Backup != nil {
		backup := *source.Backup
		backup.Sources = append([]BackupSourceSpec(nil), source.Backup.Sources...)
		clone.Backup = &backup
	}
	if source.ReleaseGroups != nil {
		clone.ReleaseGroups = make(map[string]ReleaseGroupSpec, len(source.ReleaseGroups))
		for name, group := range source.ReleaseGroups {
			group.Services = append([]string(nil), group.Services...)
			group.Order = append([]string(nil), group.Order...)
			clone.ReleaseGroups[name] = group
		}
	}
	return clone
}
