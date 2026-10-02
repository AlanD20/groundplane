package blueprints

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/core"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func encodeEnvironmentDesiredMutationAudit(value EnvironmentDesiredMutationAudit) ([]byte, error) {
	if err := ValidateEnvironmentDesiredMutationAudit(value); err != nil {
		return nil, err
	}
	body := blueprintRecordWriter{}
	body.uint16(environmentBlueprintRecordSchema)
	family := uint8(1)
	var action uint8
	if value.ConfigRestore != nil {
		family = 7
		body.string(value.ConfigRestore.BaseRevisionID)
		body.string(value.ConfigRestore.PointID)
		body.string(value.ConfigRestore.GenerationID)
		body.digest(value.ConfigRestore.SourceSHA256)
	} else if value.Service != nil {
		family, action = 2, uint8(value.Service.Action)
		body.string(value.Service.BaseRevisionID)
		body.string(value.Service.ServiceID)
		request, err := encodeEnvironmentMutationRequest(value.Service.Request)
		if err != nil {
			return nil, err
		}
		body.bytes(request)
		clear(request)
	} else if value.Entry != nil {
		family, action = 3, uint8(value.Entry.Action)
		body.string(value.Entry.BaseRevisionID)
		body.string(value.Entry.EntryID)
		record, err := encodeEnvironmentMutationRequest(value.Entry.Record)
		if err != nil {
			return nil, err
		}
		body.bytes(record)
		clear(record)
	} else if len(value.Entries) != 0 {
		family = 4
		body.uint16(uint16(len(value.Entries)))
		for _, entry := range value.Entries {
			body.uint8(uint8(entry.Action))
			body.string(entry.BaseRevisionID)
			body.string(entry.EntryID)
			record, err := encodeEnvironmentMutationRequest(entry.Record)
			if err != nil {
				return nil, err
			}
			body.bytes(record)
			clear(record)
		}
	} else if value.Zone != nil {
		family, action = 5, uint8(value.Zone.Action)
		body.string(value.Zone.BaseRevisionID)
		body.string(value.Zone.ZoneID)
		request, err := encodeEnvironmentMutationRequest(value.Zone.Request)
		if err != nil {
			return nil, err
		}
		body.bytes(request)
		clear(request)
	} else if value.Route != nil {
		family, action = 6, uint8(value.Route.Action)
		body.string(value.Route.BaseRevisionID)
		body.string(value.Route.RouteID)
		request, err := encodeEnvironmentMutationRequest(value.Route.Request)
		if err != nil {
			return nil, err
		}
		body.bytes(request)
		clear(request)
	} else {
		volume := value.Volume
		action = uint8(volume.Action)
		body.string(volume.VolumeID)
		body.string(volume.Slug)
		body.string(volume.Key)
		if volume.KeySupplied {
			body.uint8(1)
		} else {
			body.uint8(0)
		}
		body.digest(volume.PreconditionDigest)
	}
	if body.err != nil {
		clear(body.value)
		return nil, body.err
	}
	stream := make([]byte, 12, 12+len(body.value))
	copy(stream[:4], []byte("GPMU"))
	binary.BigEndian.PutUint16(stream[4:6], environmentBlueprintRecordSchema)
	stream[6], stream[7] = family, action
	binary.BigEndian.PutUint32(stream[8:12], uint32(len(body.value)))
	stream = append(stream, body.value...)
	clear(body.value)
	return stream, nil
}

func decodeEnvironmentDesiredMutationAudit(value []byte) (EnvironmentDesiredMutationAudit, error) {
	if len(value) < 12 || string(value[:4]) != "GPMU" ||
		binary.BigEndian.Uint16(value[4:6]) != environmentBlueprintRecordSchema ||
		value[6] < 1 || value[6] > 7 ||
		int(binary.BigEndian.Uint32(value[8:12])) != len(value)-12 {
		return EnvironmentDesiredMutationAudit{}, CorruptEnvironmentBlueprintStage()
	}
	reader := blueprintRecordReader{value: value[12:]}
	if reader.uint16() != environmentBlueprintRecordSchema {
		return EnvironmentDesiredMutationAudit{}, CorruptEnvironmentBlueprintStage()
	}
	result := EnvironmentDesiredMutationAudit{}
	if value[6] == 1 {
		volume := &EnvironmentVolumeMutationAudit{
			Action: EnvironmentVolumeMutationAction(value[7]), VolumeID: reader.string(128),
			Slug: reader.string(63), Key: reader.string(255),
		}
		keySupplied := reader.uint8()
		if keySupplied > 1 {
			return EnvironmentDesiredMutationAudit{}, CorruptEnvironmentBlueprintStage()
		}
		volume.KeySupplied = keySupplied == 1
		volume.PreconditionDigest = reader.digest()
		result.Volume = volume
	} else if value[6] == 2 {
		service := &EnvironmentServiceMutationAudit{
			Action:         EnvironmentServiceMutationAction(value[7]),
			BaseRevisionID: reader.string(128), ServiceID: reader.string(128),
		}
		var err error
		service.Request, err = decodeEnvironmentMutationRequest[EnvironmentServiceMutationRequest](&reader)
		if err != nil {
			return EnvironmentDesiredMutationAudit{}, err
		}
		result.Service = service
	} else if value[6] == 3 {
		entry := &EnvironmentEntryMutationAudit{
			Action:         EnvironmentEntryMutationAction(value[7]),
			BaseRevisionID: reader.string(128),
			EntryID:        reader.string(128),
		}
		var err error
		entry.Record, err = decodeEnvironmentMutationRequest[entryrecord.Record](&reader)
		if err != nil {
			return EnvironmentDesiredMutationAudit{}, err
		}
		result.Entry = entry
	} else if value[6] == 4 {
		count := reader.uint16()
		if count == 0 || count > core.MaximumBulkEntryCount {
			return EnvironmentDesiredMutationAudit{}, CorruptEnvironmentBlueprintStage()
		}
		result.Entries = make([]EnvironmentEntryMutationAudit, int(count))
		for index := range result.Entries {
			entry := EnvironmentEntryMutationAudit{
				Action:         EnvironmentEntryMutationAction(reader.uint8()),
				BaseRevisionID: reader.string(128),
				EntryID:        reader.string(128),
			}
			var err error
			entry.Record, err = decodeEnvironmentMutationRequest[entryrecord.Record](&reader)
			if err != nil {
				return EnvironmentDesiredMutationAudit{}, err
			}
			result.Entries[index] = entry
		}
	} else if value[6] == 5 {
		zone := &EnvironmentZoneMutationAudit{
			Action: EnvironmentZoneMutationAction(value[7]), BaseRevisionID: reader.string(128),
			ZoneID: reader.string(128),
		}
		var err error
		zone.Request, err = decodeEnvironmentMutationRequest[EnvironmentZoneMutationRequest](&reader)
		if err != nil {
			return EnvironmentDesiredMutationAudit{}, err
		}
		result.Zone = zone
	} else if value[6] == 6 {
		route := &EnvironmentRouteMutationAudit{
			Action: EnvironmentRouteMutationAction(value[7]), BaseRevisionID: reader.string(128),
			RouteID: reader.string(128),
		}
		var err error
		route.Request, err = decodeEnvironmentMutationRequest[EnvironmentRouteMutationRequest](&reader)
		if err != nil {
			return EnvironmentDesiredMutationAudit{}, err
		}
		result.Route = route
	} else {
		if value[7] != 0 {
			return EnvironmentDesiredMutationAudit{}, CorruptEnvironmentBlueprintStage()
		}
		result.ConfigRestore = &EnvironmentConfigRestoreAudit{
			BaseRevisionID: reader.string(128), PointID: reader.string(128),
			GenerationID: reader.string(128), SourceSHA256: reader.digest(),
		}
	}
	if reader.done() != nil || ValidateEnvironmentDesiredMutationAudit(result) != nil {
		return EnvironmentDesiredMutationAudit{}, CorruptEnvironmentBlueprintStage()
	}
	return result, nil
}

func encodeEnvironmentMutationRequest[T any](value *T) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return encoded, nil
}

func decodeEnvironmentMutationRequest[T any](reader *blueprintRecordReader) (*T, error) {
	encoded := reader.bytes(64 * 1024)
	defer clear(encoded)
	if len(encoded) == 0 {
		return nil, nil
	}
	var result T
	if json.Unmarshal(encoded, &result) != nil {
		return nil, CorruptEnvironmentBlueprintStage()
	}
	canonical, err := json.Marshal(&result)
	defer clear(canonical)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return nil, CorruptEnvironmentBlueprintStage()
	}
	return &result, nil
}
