package backupconfig

import (
	"encoding/hex"
	"strconv"
	"unicode/utf8"
)

func appendManifestEntry(destination []byte, entry Entry) []byte {
	destination = append(destination, `{"exposure":`...)
	destination = appendExposure(destination, entry.Exposure)
	destination = append(destination, `,"id":`...)
	destination = appendJSONString(destination, entry.ID)
	destination = append(destination, `,"metadata":`...)
	destination = appendMetadata(destination, entry.Metadata)
	destination = append(destination, `,"secret":`...)
	destination = strconv.AppendBool(destination, entry.Secret)
	destination = append(destination, `,"source":`...)
	destination = appendSource(destination, entry.Source)
	destination = append(destination, `,"value":{"path":`...)
	destination = appendJSONString(destination, entry.Value.Path)
	destination = append(destination, `,"sha256":`...)
	destination = appendJSONString(destination, hex.EncodeToString(entry.Value.SHA256[:]))
	destination = append(destination, `,"size_bytes":`...)
	destination = strconv.AppendUint(destination, entry.Value.SizeBytes, 10)
	destination = append(destination, "}}"...)
	return destination
}

func appendMetadata(destination []byte, metadata Metadata) []byte {
	if metadata.Kind == MetadataEnvironment {
		destination = append(destination, `{"key":`...)
		destination = appendJSONString(destination, metadata.Environment.Key)
		return append(destination, `,"type":"env"}`...)
	}
	destination = append(destination, `{"gid":`...)
	destination = strconv.AppendUint(destination, uint64(metadata.File.GID), 10)
	destination = append(destination, `,"mode":`...)
	destination = strconv.AppendUint(destination, uint64(metadata.File.Mode), 10)
	destination = append(destination, `,"path":`...)
	destination = appendJSONString(destination, metadata.File.Path)
	destination = append(destination, `,"type":"file","uid":`...)
	destination = strconv.AppendUint(destination, uint64(metadata.File.UID), 10)
	return append(destination, '}')
}

func appendExposure(destination []byte, exposure Exposure) []byte {
	if exposure.Kind == ExposureAll {
		return append(destination, `{"kind":"all"}`...)
	}
	destination = append(destination, `{"kind":"services","service_ids":[`...)
	for index, serviceID := range exposure.ServiceIDs {
		if index != 0 {
			destination = append(destination, ',')
		}
		destination = appendJSONString(destination, serviceID)
	}
	return append(destination, "]}"...)
}

func appendSource(destination []byte, source Source) []byte {
	switch source.Kind {
	case SourceLiteral:
		return append(destination, `{"kind":"literal"}`...)
	case SourceSecretReference:
		destination = append(destination, `{"kind":"secret_ref","secret_ref":`...)
		destination = appendJSONString(destination, source.SecretReference.AuthoredKey)
		return append(destination, '}')
	default:
		destination = append(destination, `{"attach_id":`...)
		destination = appendJSONString(destination, source.Fact.AttachID)
		destination = append(destination, `,"fact":`...)
		destination = appendJSONString(destination, source.Fact.Fact)
		if source.Fact.GrantAttachID != "" {
			destination = append(destination, `,"grant_attach_id":`...)
			destination = appendJSONString(destination, source.Fact.GrantAttachID)
		}
		return append(destination, `,"kind":"fact"}`...)
	}
}

func appendJSONString(destination []byte, value string) []byte {
	destination = append(destination, '"')
	for _, character := range value {
		switch character {
		case '"', '\\':
			destination = append(destination, '\\', byte(character))
		case '\b':
			destination = append(destination, `\b`...)
		case '\t':
			destination = append(destination, `\t`...)
		case '\n':
			destination = append(destination, `\n`...)
		case '\f':
			destination = append(destination, `\f`...)
		case '\r':
			destination = append(destination, `\r`...)
		default:
			if character < 0x20 {
				destination = append(destination, `\u00`...)
				const hexadecimal = "0123456789abcdef"
				destination = append(
					destination,
					hexadecimal[byte(character)>>4],
					hexadecimal[byte(character)&15],
				)
			} else {
				destination = utf8.AppendRune(destination, character)
			}
		}
	}
	return append(destination, '"')
}
