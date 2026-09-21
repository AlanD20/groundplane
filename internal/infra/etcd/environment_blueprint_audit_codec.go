package etcd

import (
	"crypto/sha256"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"sort"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func encodeEnvironmentBlueprintAuditStream(revision blueprints.EnvironmentBlueprintRevision) ([]byte, error) {
	if err := blueprints.ValidateEnvironmentBlueprintRevision(revision); err != nil {
		return nil, err
	}
	writer := blueprintRecordWriter{value: []byte("GPAU")}
	writer.uint16(1)
	writer.string(revision.EnvironmentID)
	writer.string(revision.RevisionID)
	writer.string(revision.RootPath)
	writer.timestamp(revision.CreatedAt)
	writer.uint16(uint16(len(revision.ComposeSources)))
	for _, source := range revision.ComposeSources {
		writer.string(source)
	}
	interpolationKeys := make([]string, 0, len(revision.Interpolation))
	for key := range revision.Interpolation {
		interpolationKeys = append(interpolationKeys, key)
	}
	sort.Strings(interpolationKeys)
	writer.uint16(uint16(len(interpolationKeys)))
	for _, key := range interpolationKeys {
		writer.string(key)
		writer.string(revision.Interpolation[key])
	}
	writer.uint16(uint16(len(revision.Files)))
	for _, file := range revision.Files {
		writer.string(file.Path)
		writer.digest(sha256.Sum256(file.Content))
		writer.bytes(file.Content)
	}
	if writer.err != nil {
		clear(writer.value)
		return nil, writer.err
	}
	if len(writer.value) > environmentBlueprintMaximumAuditBytes {
		clear(writer.value)
		return nil, errs.New(errs.KindValidationFailed, "Blueprint audit stream exceeds its accepted ceiling")
	}
	return writer.value, nil
}

func decodeEnvironmentBlueprintAuditStream(value []byte) (blueprints.EnvironmentBlueprintRevision, error) {
	if len(value) < 6 || len(value) > environmentBlueprintMaximumAuditBytes || string(value[:4]) != "GPAU" {
		return blueprints.EnvironmentBlueprintRevision{}, blueprints.CorruptEnvironmentBlueprint()
	}
	reader := blueprintRecordReader{value: value[4:]}
	if reader.uint16() != 1 {
		return blueprints.EnvironmentBlueprintRevision{}, blueprints.CorruptEnvironmentBlueprint()
	}
	revision := blueprints.EnvironmentBlueprintRevision{
		EnvironmentID: reader.string(128),
		RevisionID:    reader.string(128),
		RootPath:      reader.string(blueprints.EnvironmentBlueprintMaxPathBytes),
		CreatedAt:     reader.timestamp(),
	}
	sourceCount := int(reader.uint16())
	if reader.err != nil || sourceCount > blueprints.EnvironmentBlueprintMaxFiles {
		return blueprints.EnvironmentBlueprintRevision{}, blueprints.CorruptEnvironmentBlueprint()
	}
	revision.ComposeSources = make([]string, sourceCount)
	for index := range revision.ComposeSources {
		revision.ComposeSources[index] = reader.string(blueprints.EnvironmentBlueprintMaxPathBytes)
	}
	interpolationCount := int(reader.uint16())
	if reader.err != nil || interpolationCount > 256 {
		return blueprints.EnvironmentBlueprintRevision{}, blueprints.CorruptEnvironmentBlueprint()
	}
	revision.Interpolation = make(map[string]string, interpolationCount)
	previousKey := ""
	for index := 0; index < interpolationCount; index++ {
		key := reader.string(128)
		item := reader.string(4096)
		if reader.err != nil || key <= previousKey {
			return blueprints.EnvironmentBlueprintRevision{}, blueprints.CorruptEnvironmentBlueprint()
		}
		revision.Interpolation[key] = item
		previousKey = key
	}
	fileCount := int(reader.uint16())
	if reader.err != nil || fileCount > blueprints.EnvironmentBlueprintMaxFiles {
		return blueprints.EnvironmentBlueprintRevision{}, blueprints.CorruptEnvironmentBlueprint()
	}
	revision.Files = make([]blueprints.EnvironmentBlueprintFile, fileCount)
	for index := range revision.Files {
		path := reader.string(blueprints.EnvironmentBlueprintMaxPathBytes)
		digest := reader.digest()
		content := reader.bytes(blueprints.EnvironmentBlueprintMaxFileBytes)
		if reader.err != nil || sha256.Sum256(content) != digest {
			clear(content)
			for previous := 0; previous < index; previous++ {
				clear(revision.Files[previous].Content)
			}
			return blueprints.EnvironmentBlueprintRevision{}, blueprints.CorruptEnvironmentBlueprint()
		}
		revision.Files[index] = blueprints.EnvironmentBlueprintFile{Path: path, Content: content}
	}
	if reader.done() != nil || blueprints.ValidateEnvironmentBlueprintRevision(revision) != nil {
		for index := range revision.Files {
			clear(revision.Files[index].Content)
		}
		return blueprints.EnvironmentBlueprintRevision{}, blueprints.CorruptEnvironmentBlueprint()
	}
	return revision, nil
}
