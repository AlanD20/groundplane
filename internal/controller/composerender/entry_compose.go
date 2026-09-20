package composerender

import (
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	environmentfile "github.com/AlanD20/groundplane/internal/controller/environmentfile"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"path"
	"path/filepath"
	"sort"
)

// EnvironmentEntryMaterialization is one immutable Entry-derived file that
// must exist before Compose starts the projected Environment.
type EnvironmentEntryMaterialization struct {
	Destination string
	ServiceID   string
	ServiceName string
	OutputKind  etcd.TaskMaterializationOutputKind
	UID         uint32
	GID         uint32
	Mode        entrymaterialization.Mode
	Source      etcd.TaskMaterializationSource
}

// EnvironmentEntryComposeProjection is the Entry-derived addition to one
// Compose project. Project is an owned clone and Materializations contain
// immutable value references, never value bytes.
type EnvironmentEntryComposeProjection struct {
	Project          *composetypes.Project
	Materializations []EnvironmentEntryMaterialization
}

// ProjectEnvironmentEntries compiles the unified Environment Entry model into
// explicit Compose env_file and bind-mount declarations plus Agent file
// materializations. Component-generated services own their service-specific
// inputs; the canonical all-services file still reaches every active service.
func ProjectEnvironmentEntries(
	project *composetypes.Project,
	environmentID string,
	volumeDir string,
	authoredServices []composeidentity.Resource,
	entries []entryrecord.Record,
) (EnvironmentEntryComposeProjection, error) {
	if project == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		!filepath.IsAbs(
			volumeDir,
		) || filepath.Clean(volumeDir) != volumeDir || filepath.Base(volumeDir) != environmentID {
		return EnvironmentEntryComposeProjection{}, errs.New(
			errs.KindInternal,
			"Environment Entry Compose input is invalid",
		)
	}
	projected := cloneComposeProjectServices(project)
	serviceIDs := make(map[string]string, len(authoredServices))
	for _, identity := range authoredServices {
		if ids.Validate(ids.KindService, identity.ID) != nil || !core.ValidEnvironmentComposeName(identity.Name) {
			return EnvironmentEntryComposeProjection{}, errs.New(
				errs.KindInternal,
				"Environment Entry Service identity is invalid",
			)
		}
		if _, _, exists := environmentComposeService(projected, identity.Name); !exists {
			return EnvironmentEntryComposeProjection{}, errs.New(
				errs.KindInternal,
				"Environment Entry Service identity is absent from Compose",
			)
		}
		if _, duplicate := serviceIDs[identity.Name]; duplicate {
			return EnvironmentEntryComposeProjection{}, errs.New(
				errs.KindInternal,
				"Environment Entry Service identity is duplicated",
			)
		}
		serviceIDs[identity.Name] = identity.ID
	}

	allValues := make([]etcd.TaskGeneratedEnvironmentEntryReference, 0)
	serviceValues := make(map[string][]etcd.TaskGeneratedEnvironmentEntryReference)
	materializations := make([]EnvironmentEntryMaterialization, 0, len(entries)+1)
	for _, record := range entries {
		if record.EnvironmentID != environmentID || record.Entry.Validate() != nil ||
			ids.Validate(ids.KindConfig, record.CurrentValueGenerationID) != nil {
			return EnvironmentEntryComposeProjection{}, errs.New(
				errs.KindInternal,
				"Environment Entry projection contains an invalid record",
			)
		}
		entry := record.Entry
		storage := etcd.TaskEntryValueStoragePlain
		if entry.Secret {
			storage = etcd.TaskEntryValueStorageSecret
		}
		value := etcd.TaskEntryValueReference{
			EntryID: entry.ID, ValueGenerationID: record.CurrentValueGenerationID, Storage: storage,
		}
		if entry.Kind == core.EntryKindEnv {
			reference := etcd.TaskGeneratedEnvironmentEntryReference{Name: entry.Key, Value: value}
			if entry.ExposesAll() {
				allValues = append(allValues, reference)
				continue
			}
			for _, serviceName := range entry.Exposure {
				if _, _, exists := environmentComposeService(projected, serviceName); !exists {
					return EnvironmentEntryComposeProjection{}, errs.New(
						errs.KindValidationFailed,
						"Environment Entry exposure references an unknown Service",
					)
				}
				if _, authored := serviceIDs[serviceName]; authored {
					serviceValues[serviceName] = append(serviceValues[serviceName], reference)
				}
			}
			continue
		}
		if entry.Kind != core.EntryKindFile || entry.UID == nil || entry.GID == nil ||
			entrymaterialization.ValidateDesiredDestination(entry.Path) != nil {
			return EnvironmentEntryComposeProjection{}, errs.New(
				errs.KindInternal,
				"Environment file Entry projection is invalid",
			)
		}
		outputKind := etcd.TaskMaterializationOutputPlainFile
		mode := entrymaterialization.ModeReadOnly
		if entry.Secret {
			outputKind = etcd.TaskMaterializationOutputSecretFile
			mode = entrymaterialization.ModePrivate
		}
		materializations = append(materializations, EnvironmentEntryMaterialization{
			Destination: entry.Path, OutputKind: outputKind, UID: *entry.UID, GID: *entry.GID, Mode: mode,
			Source: etcd.TaskMaterializationSource{
				Kind: etcd.TaskMaterializationSourceEntryValue, EntryValue: &value,
			},
		})
		targets := entry.Exposure
		if entry.ExposesAll() {
			targets = sortedEnvironmentComposeServiceNames(projected)
		}
		for _, serviceName := range targets {
			service, active, exists := environmentComposeService(projected, serviceName)
			if !exists {
				return EnvironmentEntryComposeProjection{}, errs.New(
					errs.KindValidationFailed,
					"Environment file Entry exposure references an unknown Service",
				)
			}
			if _, authored := serviceIDs[serviceName]; !authored && !entry.ExposesAll() {
				continue
			}
			mount := composetypes.ServiceVolumeConfig{
				Type:   composetypes.VolumeTypeBind,
				Source: filepath.Join(volumeDir, filepath.FromSlash(entry.Path)),
				Target: path.Join("/", entry.Path), ReadOnly: true,
				Bind: &composetypes.ServiceVolumeBind{CreateHostPath: false},
			}
			for _, existing := range service.Volumes {
				if existing.Target == mount.Target {
					return EnvironmentEntryComposeProjection{}, errs.New(
						errs.KindNameConflict,
						"Environment file Entry conflicts with an existing Service mount",
					)
				}
			}
			service.Volumes = append(append([]composetypes.ServiceVolumeConfig(nil), service.Volumes...), mount)
			setEnvironmentComposeService(projected, serviceName, service, active)
		}
	}

	sortGeneratedEnvironmentValues(allValues)
	if duplicateGeneratedEnvironmentName(allValues) {
		return EnvironmentEntryComposeProjection{}, errs.New(
			errs.KindNameConflict,
			"all-services Environment Entries contain a duplicate key",
		)
	}
	canonicalDestination := environmentfile.EnvFileName(environmentID)
	materializations = append(materializations, EnvironmentEntryMaterialization{
		Destination: canonicalDestination,
		OutputKind:  etcd.TaskMaterializationOutputGeneratedEnvironment,
		Mode:        entrymaterialization.ModePrivate,
		Source: etcd.TaskMaterializationSource{
			Kind: etcd.TaskMaterializationSourceGeneratedEnvironment,
			GeneratedEnvironment: &etcd.TaskGeneratedEnvironmentValueReference{
				FormatVersion: 1, Values: allValues,
			},
		},
	})
	canonicalPath := filepath.Join(volumeDir, filepath.FromSlash(canonicalDestination))
	for _, serviceName := range sortedEnvironmentComposeServiceNames(projected) {
		service, active, _ := environmentComposeService(projected, serviceName)
		service.EnvFiles = prependRequiredEnvFile(service.EnvFiles, canonicalPath)
		setEnvironmentComposeService(projected, serviceName, service, active)
	}

	serviceNames := make([]string, 0, len(serviceValues))
	for serviceName := range serviceValues {
		serviceNames = append(serviceNames, serviceName)
	}
	sort.Strings(serviceNames)
	for _, serviceName := range serviceNames {
		values := serviceValues[serviceName]
		sortGeneratedEnvironmentValues(values)
		if duplicateGeneratedEnvironmentName(values) {
			return EnvironmentEntryComposeProjection{}, errs.New(
				errs.KindNameConflict,
				"service Environment Entries contain a duplicate key",
			)
		}
		destination := environmentfile.ServiceEnvFileName(environmentID, serviceName)
		materializations = append(materializations, EnvironmentEntryMaterialization{
			Destination: destination, ServiceID: serviceIDs[serviceName], ServiceName: serviceName,
			OutputKind: etcd.TaskMaterializationOutputGeneratedEnvironment,
			Mode:       entrymaterialization.ModePrivate,
			Source: etcd.TaskMaterializationSource{
				Kind: etcd.TaskMaterializationSourceGeneratedEnvironment,
				GeneratedEnvironment: &etcd.TaskGeneratedEnvironmentValueReference{
					FormatVersion: 1, Values: values,
				},
			},
		})
		service, active, _ := environmentComposeService(projected, serviceName)
		service.EnvFiles = appendRequiredEnvFile(
			service.EnvFiles,
			filepath.Join(volumeDir, filepath.FromSlash(destination)),
		)
		setEnvironmentComposeService(projected, serviceName, service, active)
	}
	sort.Slice(materializations, func(left int, right int) bool {
		return materializations[left].Destination < materializations[right].Destination
	})
	for index := 1; index < len(materializations); index++ {
		if materializations[index].Destination == materializations[index-1].Destination {
			return EnvironmentEntryComposeProjection{}, errs.New(
				errs.KindNameConflict,
				"Environment Entry materialization destination is duplicated",
			)
		}
	}
	return EnvironmentEntryComposeProjection{Project: projected, Materializations: materializations}, nil
}

func sortedEnvironmentComposeServiceNames(project *composetypes.Project) []string {
	names := make([]string, 0, len(project.Services)+len(project.DisabledServices))
	for name := range project.Services {
		names = append(names, name)
	}
	for name := range project.DisabledServices {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func environmentComposeService(
	project *composetypes.Project,
	name string,
) (composetypes.ServiceConfig, bool, bool) {
	service, active := project.Services[name]
	if active {
		return service, true, true
	}
	service, exists := project.DisabledServices[name]
	return service, false, exists
}

func setEnvironmentComposeService(
	project *composetypes.Project,
	name string,
	service composetypes.ServiceConfig,
	active bool,
) {
	if active {
		project.Services[name] = service
		return
	}
	project.DisabledServices[name] = service
}

func sortGeneratedEnvironmentValues(values []etcd.TaskGeneratedEnvironmentEntryReference) {
	sort.Slice(values, func(left int, right int) bool { return values[left].Name < values[right].Name })
}

func duplicateGeneratedEnvironmentName(values []etcd.TaskGeneratedEnvironmentEntryReference) bool {
	for index := 1; index < len(values); index++ {
		if values[index].Name == values[index-1].Name {
			return true
		}
	}
	return false
}

func prependRequiredEnvFile(existing []composetypes.EnvFile, filePath string) []composetypes.EnvFile {
	result := make([]composetypes.EnvFile, 0, len(existing)+1)
	result = append(result, composetypes.EnvFile{Path: filePath, Required: true})
	for _, envFile := range existing {
		if envFile.Path != filePath {
			result = append(result, envFile)
		}
	}
	return result
}

func appendRequiredEnvFile(existing []composetypes.EnvFile, filePath string) []composetypes.EnvFile {
	result := make([]composetypes.EnvFile, 0, len(existing)+1)
	for _, envFile := range existing {
		if envFile.Path != filePath {
			result = append(result, envFile)
		}
	}
	return append(result, composetypes.EnvFile{Path: filePath, Required: true})
}
