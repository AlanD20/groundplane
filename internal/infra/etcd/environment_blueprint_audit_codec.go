package etcd

import (
	"crypto/sha256"
	"sort"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func encodeEnvironmentBlueprintAuditStream(revision EnvironmentBlueprintRevision) ([]byte, error) {
	if err := validateEnvironmentBlueprintRevision(revision); err != nil {
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

func decodeEnvironmentBlueprintAuditStream(value []byte) (EnvironmentBlueprintRevision, error) {
	if len(value) < 6 || len(value) > environmentBlueprintMaximumAuditBytes || string(value[:4]) != "GPAU" {
		return EnvironmentBlueprintRevision{}, corruptEnvironmentBlueprint()
	}
	reader := blueprintRecordReader{value: value[4:]}
	if reader.uint16() != 1 {
		return EnvironmentBlueprintRevision{}, corruptEnvironmentBlueprint()
	}
	revision := EnvironmentBlueprintRevision{
		EnvironmentID: reader.string(128),
		RevisionID:    reader.string(128),
		RootPath:      reader.string(environmentBlueprintMaxPathBytes),
		CreatedAt:     reader.timestamp(),
	}
	sourceCount := int(reader.uint16())
	if reader.err != nil || sourceCount > environmentBlueprintMaxFiles {
		return EnvironmentBlueprintRevision{}, corruptEnvironmentBlueprint()
	}
	revision.ComposeSources = make([]string, sourceCount)
	for index := range revision.ComposeSources {
		revision.ComposeSources[index] = reader.string(environmentBlueprintMaxPathBytes)
	}
	interpolationCount := int(reader.uint16())
	if reader.err != nil || interpolationCount > 256 {
		return EnvironmentBlueprintRevision{}, corruptEnvironmentBlueprint()
	}
	revision.Interpolation = make(map[string]string, interpolationCount)
	previousKey := ""
	for index := 0; index < interpolationCount; index++ {
		key := reader.string(128)
		item := reader.string(4096)
		if reader.err != nil || key <= previousKey {
			return EnvironmentBlueprintRevision{}, corruptEnvironmentBlueprint()
		}
		revision.Interpolation[key] = item
		previousKey = key
	}
	fileCount := int(reader.uint16())
	if reader.err != nil || fileCount > environmentBlueprintMaxFiles {
		return EnvironmentBlueprintRevision{}, corruptEnvironmentBlueprint()
	}
	revision.Files = make([]EnvironmentBlueprintFile, fileCount)
	for index := range revision.Files {
		path := reader.string(environmentBlueprintMaxPathBytes)
		digest := reader.digest()
		content := reader.bytes(environmentBlueprintMaxFileBytes)
		if reader.err != nil || sha256.Sum256(content) != digest {
			clear(content)
			for previous := 0; previous < index; previous++ {
				clear(revision.Files[previous].Content)
			}
			return EnvironmentBlueprintRevision{}, corruptEnvironmentBlueprint()
		}
		revision.Files[index] = EnvironmentBlueprintFile{Path: path, Content: content}
	}
	if reader.done() != nil || validateEnvironmentBlueprintRevision(revision) != nil {
		for index := range revision.Files {
			clear(revision.Files[index].Content)
		}
		return EnvironmentBlueprintRevision{}, corruptEnvironmentBlueprint()
	}
	return revision, nil
}
