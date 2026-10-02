package backupconfigmaterialization

import (
	"bytes"
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type output struct {
	metadata entrymaterialization.MetadataSpec
	values   []int
}

type Plan struct {
	authority *agentpb.BackupConfigRestoreAuthority
	entries   []backupconfig.Entry
	outputs   []output
}

// NewPlan checks the complete canonical manifest, then derives only restored
// outputs and removals owned by the sealed predecessor. No current host list,
// Secret or desired-state lookup can expand its authority.
func NewPlan(
	ctx context.Context,
	authority *agentpb.BackupConfigRestoreAuthority,
	entries []backupconfig.Entry,
) (*Plan, error) {
	if ctx == nil || authority == nil || authority.RenderGeneration == 0 ||
		ValidateContext(authority.DestinationEnvironmentId, authority.Files) != nil {
		return nil, invalid()
	}
	actual, err := backupconfig.BuildContentAuthority(ctx, entries)
	expected := authority.GetExpectedArchive().GetContent()
	if err != nil || expected == nil || !bytes.Equal(actual.ManifestSHA256[:], expected.ManifestSha256) ||
		actual.EntryCount != expected.EntryCount || actual.TotalSelectedValueBytes != expected.TotalSelectedValueBytes ||
		actual.ManifestSizeBytes != expected.ManifestSizeBytes || actual.SourceSizeBytes != expected.SourceSizeBytes {
		return nil, invalid()
	}
	plan := &Plan{authority: proto.CloneOf(authority), entries: append([]backupconfig.Entry(nil), entries...)}
	for index := range plan.entries {
		plan.entries[index].Exposure.ServiceIDs = append([]string(nil), entries[index].Exposure.ServiceIDs...)
	}
	services := make(map[string]*agentpb.BackupConfigFileService, len(authority.Files.Services))
	for _, service := range authority.Files.Services {
		services[service.ServiceId] = service
	}
	global, err := entrymaterialization.GeneratedEnvDestination(authority.DestinationEnvironmentId, "")
	if err != nil {
		return nil, err
	}
	byPath := map[string]output{global: {metadata: plan.metadata(global, entrymaterialization.OutputGeneratedEnv)}}
	for index, entry := range entries {
		for _, serviceID := range entry.Exposure.ServiceIDs {
			if services[serviceID] == nil {
				return nil, invalid()
			}
		}
		if entry.Metadata.Kind == backupconfig.MetadataFile {
			file := entry.Metadata.File
			if _, duplicate := byPath[file.Path]; duplicate {
				return nil, invalid()
			}
			kind := entrymaterialization.OutputPlainFile
			if entry.Secret {
				kind = entrymaterialization.OutputSecretFile
			}
			metadata := plan.metadata(file.Path, kind)
			metadata.UID, metadata.GID = file.UID, file.GID
			byPath[file.Path] = output{metadata: metadata, values: []int{index}}
			continue
		}
		if entry.Exposure.Kind == backupconfig.ExposureAll {
			item := byPath[global]
			item.values = append(item.values, index)
			byPath[global] = item
			continue
		}
		for _, serviceID := range entry.Exposure.ServiceIDs {
			service := services[serviceID]
			destination, err := entrymaterialization.GeneratedEnvDestination(
				authority.DestinationEnvironmentId,
				service.Name,
			)
			if err != nil {
				return nil, err
			}
			item, found := byPath[destination]
			if !found {
				item.metadata = plan.metadata(destination, entrymaterialization.OutputGeneratedEnv)
				item.metadata.ServiceID, item.metadata.ServiceName = service.ServiceId, service.Name
			} else if item.metadata.OutputKind != entrymaterialization.OutputGeneratedEnv {
				return nil, invalid()
			}
			item.values = append(item.values, index)
			byPath[destination] = item
		}
	}
	for _, file := range authority.Files.PriorFiles {
		if _, restored := byPath[file.Destination]; restored {
			continue
		}
		kind := entrymaterialization.OutputRemovePlainFile
		if file.Secret {
			kind = entrymaterialization.OutputRemoveSecretFile
		}
		metadata := plan.metadata(file.Destination, kind)
		metadata.UID, metadata.GID = file.Uid, file.Gid
		byPath[file.Destination] = output{metadata: metadata}
	}
	for _, service := range authority.Files.Services {
		if !service.HadEnvironmentFile {
			continue
		}
		destination, err := entrymaterialization.GeneratedEnvDestination(
			authority.DestinationEnvironmentId,
			service.Name,
		)
		if err != nil {
			return nil, err
		}
		if _, restored := byPath[destination]; restored {
			continue
		}
		metadata := plan.metadata(destination, entrymaterialization.OutputRemoveGeneratedEnv)
		metadata.ServiceID, metadata.ServiceName = service.ServiceId, service.Name
		byPath[destination] = output{metadata: metadata}
	}
	for _, item := range byPath {
		if entrymaterialization.ValidateMetadata(item.metadata) != nil {
			return nil, invalid()
		}
		plan.outputs = append(plan.outputs, item)
	}
	sort.Slice(plan.outputs, func(i, j int) bool {
		return plan.outputs[i].metadata.Destination < plan.outputs[j].metadata.Destination
	})
	return plan, nil
}

func (plan *Plan) metadata(destination string, kind entrymaterialization.OutputKind) entrymaterialization.MetadataSpec {
	mode := entrymaterialization.ModePrivate
	if kind == entrymaterialization.OutputPlainFile || kind == entrymaterialization.OutputRemovePlainFile {
		mode = entrymaterialization.ModeReadOnly
	}
	return entrymaterialization.MetadataSpec{EnvironmentID: plan.authority.DestinationEnvironmentId,
		Generation: plan.authority.RenderGeneration, Destination: destination, OutputKind: kind, Mode: mode}
}

func (plan *Plan) OutputCount() int {
	if plan == nil {
		return 0
	}
	return len(plan.outputs)
}

func (plan *Plan) VolumeDir() string {
	if plan == nil {
		return ""
	}
	return plan.authority.Files.VolumeDir
}
