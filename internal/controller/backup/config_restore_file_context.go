package backup

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/common/backupconfigmaterialization"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BuildConfigRestoreFileContext is the admission snapshot of managed files and
// stable Services. Completion reconstructs it from that exact predecessor and
// compares it with the immutable procedure, never with current desired state.
func BuildConfigRestoreFileContext(
	volumeRoot string,
	projection environmentprojection.EnvironmentComposeProjection,
) (*agentpb.BackupConfigFileContext, error) {
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(projection.ComposeArtifact, artifact) != nil ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != projection.EnvironmentID {
		return nil, configSnapshotGuardConflict()
	}
	files := &agentpb.BackupConfigFileContext{VolumeRoot: volumeRoot, VolumeDir: artifact.GetAuthorizedVolumeDir()}
	byName := make(map[string]*agentpb.BackupConfigFileService, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		if service.EnvironmentID != projection.EnvironmentID {
			return nil, configSnapshotGuardConflict()
		}
		item := &agentpb.BackupConfigFileService{ServiceId: service.Desired.ID, Name: service.Desired.Name}
		if byName[item.Name] != nil {
			return nil, configSnapshotGuardConflict()
		}
		byName[item.Name] = item
		files.Services = append(files.Services, item)
	}
	for _, record := range projection.Entries {
		if record.EnvironmentID != projection.EnvironmentID {
			return nil, configSnapshotGuardConflict()
		}
		entry := record.Entry
		if entry.Kind == core.EntryKindFile {
			if entry.UID == nil || entry.GID == nil {
				return nil, configSnapshotGuardConflict()
			}
			files.PriorFiles = append(files.PriorFiles, &agentpb.BackupConfigPriorFile{EntryId: entry.ID,
				Destination: entry.Path, Secret: entry.Secret, Uid: *entry.UID, Gid: *entry.GID})
		} else if entry.Kind == core.EntryKindEnv && !entry.ExposesAll() {
			for _, name := range entry.Exposure {
				service := byName[name]
				if service == nil {
					return nil, configSnapshotGuardConflict()
				}
				service.HadEnvironmentFile = true
			}
		}
	}
	sort.Slice(files.Services, func(i, j int) bool { return files.Services[i].ServiceId < files.Services[j].ServiceId })
	sort.Slice(
		files.PriorFiles,
		func(i, j int) bool { return files.PriorFiles[i].EntryId < files.PriorFiles[j].EntryId },
	)
	if err := backupconfigmaterialization.ValidateContext(projection.EnvironmentID, files); err != nil {
		return nil, err
	}
	return files, nil
}
