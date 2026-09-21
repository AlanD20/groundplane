package etcd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/core"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func decodeEnvironmentDesiredMutationAudit(value []byte) (EnvironmentDesiredMutationAudit, error) {
	if len(value) < 12 || string(value[:4]) != "GPMU" ||
		binary.BigEndian.Uint16(value[4:6]) != environmentBlueprintRecordSchema ||
		value[6] < 1 || value[6] > 6 ||
		int(binary.BigEndian.Uint32(value[8:12])) != len(value)-12 {
		return EnvironmentDesiredMutationAudit{}, corruptEnvironmentBlueprintStage()
	}
	reader := blueprintRecordReader{value: value[12:]}
	if reader.uint16() != environmentBlueprintRecordSchema {
		return EnvironmentDesiredMutationAudit{}, corruptEnvironmentBlueprintStage()
	}
	result := EnvironmentDesiredMutationAudit{}
	if value[6] == 1 {
		volume := &EnvironmentVolumeMutationAudit{
			Action: EnvironmentVolumeMutationAction(value[7]), VolumeID: reader.string(128),
			Slug: reader.string(63), Key: reader.string(255),
		}
		keySupplied := reader.uint8()
		if keySupplied > 1 {
			return EnvironmentDesiredMutationAudit{}, corruptEnvironmentBlueprintStage()
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
			return EnvironmentDesiredMutationAudit{}, corruptEnvironmentBlueprintStage()
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
	} else {
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
	}
	if reader.done() != nil || validateEnvironmentDesiredMutationAudit(result) != nil {
		return EnvironmentDesiredMutationAudit{}, corruptEnvironmentBlueprintStage()
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
		return nil, corruptEnvironmentBlueprintStage()
	}
	canonical, err := json.Marshal(&result)
	defer clear(canonical)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return nil, corruptEnvironmentBlueprintStage()
	}
	return &result, nil
}
