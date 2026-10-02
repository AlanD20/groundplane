// Package backupconfigmaterialization derives bounded file procedures from a
// verified Config archive and its sealed predecessor file ownership. It performs
// no host or repository I/O and never selects current desired state.
package backupconfigmaterialization

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func ValidateContext(environmentID string, files *agentpb.BackupConfigFileContext) error {
	if files == nil || len(files.ProtoReflect().GetUnknown()) != 0 || len(files.Services) > 512 ||
		len(files.PriorFiles) > backupconfig.MaxEntries || proto.Size(files) > 256<<10 {
		return invalid()
	}
	scope, err := environmentpath.Parse(files.VolumeRoot, files.VolumeDir)
	if err != nil || scope.EnvironmentID != environmentID || scope.ProjectKind != environmentpath.ProjectKindTenant {
		return invalid()
	}
	names, destinations := make(map[string]struct{}), make(map[string]struct{})
	for index, service := range files.Services {
		if service == nil || service.Name == "" || len(service.ProtoReflect().GetUnknown()) != 0 ||
			ids.Validate(ids.KindService, service.ServiceId) != nil ||
			(index > 0 && files.Services[index-1].ServiceId >= service.ServiceId) {
			return invalid()
		}
		if _, duplicate := names[service.Name]; duplicate {
			return invalid()
		}
		names[service.Name] = struct{}{}
		destination, err := entrymaterialization.GeneratedEnvDestination(environmentID, service.Name)
		if err != nil || entrymaterialization.ValidateMetadata(entrymaterialization.MetadataSpec{
			EnvironmentID: environmentID, Generation: 1, Destination: destination,
			ServiceID: service.ServiceId, ServiceName: service.Name, OutputKind: entrymaterialization.OutputGeneratedEnv,
			Mode: entrymaterialization.ModePrivate}) != nil {
			return invalid()
		}
		if service.HadEnvironmentFile {
			destinations[destination] = struct{}{}
		}
	}
	global, err := entrymaterialization.GeneratedEnvDestination(environmentID, "")
	if err != nil {
		return invalid()
	}
	destinations[global] = struct{}{}
	for index, file := range files.PriorFiles {
		if file == nil || len(file.ProtoReflect().GetUnknown()) != 0 ||
			ids.Validate(ids.KindEnvEntry, file.EntryId) != nil ||
			(index > 0 && files.PriorFiles[index-1].EntryId >= file.EntryId) {
			return invalid()
		}
		kind, mode := entrymaterialization.OutputPlainFile, entrymaterialization.ModeReadOnly
		if file.Secret {
			kind, mode = entrymaterialization.OutputSecretFile, entrymaterialization.ModePrivate
		}
		if entrymaterialization.ValidateMetadata(entrymaterialization.MetadataSpec{EnvironmentID: environmentID,
			Generation: 1, Destination: file.Destination, OutputKind: kind, UID: file.Uid, GID: file.Gid, Mode: mode}) != nil {
			return invalid()
		}
		if _, duplicate := destinations[file.Destination]; duplicate {
			return invalid()
		}
		destinations[file.Destination] = struct{}{}
	}
	return nil
}

func ContextSHA256(environmentID string, files *agentpb.BackupConfigFileContext) (string, error) {
	if err := ValidateContext(environmentID, files); err != nil {
		return "", err
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(files)
	if err != nil {
		return "", invalid()
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func invalid() error {
	return errs.New(errs.KindStateConflict, "Config restore materialization differs from its sealed file authority")
}
