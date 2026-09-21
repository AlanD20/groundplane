package handlers

import (
	"encoding/json"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func decodeEntryEdit(body []byte) (apiTypes.EntryEditRequest, error) {
	members, err := decodeEntryJSONObject(body, "entry edit")
	if err != nil {
		return apiTypes.EntryEditRequest{}, err
	}
	defer clearEntryJSONMembers(members)
	for name := range members {
		if name != "source" && name != "exposure" {
			return apiTypes.EntryEditRequest{}, errs.New(
				errs.KindMalformedRequest, "entry edit body contains an unknown member",
			)
		}
	}
	for _, required := range []string{"source", "exposure"} {
		if _, present := members[required]; !present {
			return apiTypes.EntryEditRequest{}, errs.Newf(
				errs.KindValidationFailed, "entry edit requires %s", required,
			)
		}
	}
	input := apiTypes.EntryEditRequest{}
	input.Source, err = decodeEntrySource(members["source"])
	if err != nil {
		return apiTypes.EntryEditRequest{}, err
	}
	if err := decodeEntryJSONMember(members["exposure"], &input.Exposure); err != nil {
		return apiTypes.EntryEditRequest{}, err
	}
	return input, nil
}

func decodeEntryCreate(body []byte) (apiTypes.EntryCreateRequest, error) {
	members, err := decodeEntryJSONObject(body, "entry creation")
	if err != nil {
		return apiTypes.EntryCreateRequest{}, err
	}
	defer clearEntryJSONMembers(members)
	allowed := map[string]struct{}{
		"environment_id": {}, "type": {}, "key": {}, "path": {}, "uid": {}, "gid": {},
		"source": {}, "exposure": {}, "secret": {},
	}
	for name := range members {
		if _, ok := allowed[name]; !ok {
			return apiTypes.EntryCreateRequest{}, errs.New(
				errs.KindMalformedRequest,
				"entry creation body contains an unknown member",
			)
		}
	}
	for _, required := range []string{"environment_id", "type", "source", "exposure", "secret"} {
		if _, present := members[required]; !present {
			return apiTypes.EntryCreateRequest{}, errs.Newf(
				errs.KindValidationFailed,
				"entry creation requires %s",
				required,
			)
		}
	}
	input := apiTypes.EntryCreateRequest{}
	for name, target := range map[string]*string{
		"environment_id": &input.EnvironmentID, "type": &input.Type, "key": &input.Key, "path": &input.Path,
	} {
		if value, present := members[name]; present {
			if err := decodeEntryJSONMember(value, target); err != nil {
				return apiTypes.EntryCreateRequest{}, err
			}
		}
	}
	for name, target := range map[string]**int64{"uid": &input.UID, "gid": &input.GID} {
		if value, present := members[name]; present {
			var decoded int64
			if err := decodeEntryJSONMember(value, &decoded); err != nil {
				return apiTypes.EntryCreateRequest{}, err
			}
			*target = &decoded
		}
	}
	if err := decodeEntryJSONMember(members["exposure"], &input.Exposure); err != nil {
		return apiTypes.EntryCreateRequest{}, err
	}
	if err := decodeEntryJSONMember(members["secret"], &input.Secret); err != nil {
		return apiTypes.EntryCreateRequest{}, err
	}
	source, err := decodeEntrySource(members["source"])
	if err != nil {
		return apiTypes.EntryCreateRequest{}, err
	}
	input.Source = source
	return input, nil
}

func decodeEntryBulkUpsert(body []byte) (apiTypes.EntryBulkUpsertRequest, error) {
	members, err := decodeEntryJSONObject(body, "entry bulk upsert")
	if err != nil {
		return apiTypes.EntryBulkUpsertRequest{}, err
	}
	defer clearEntryJSONMembers(members)
	allowed := map[string]struct{}{
		"environment_id": {}, "entries": {}, "exposure": {}, "secret": {},
	}
	for name := range members {
		if _, ok := allowed[name]; !ok {
			return apiTypes.EntryBulkUpsertRequest{}, errs.New(
				errs.KindMalformedRequest, "entry bulk upsert body contains an unknown member",
			)
		}
	}
	for _, required := range []string{"environment_id", "entries", "exposure", "secret"} {
		if _, present := members[required]; !present {
			return apiTypes.EntryBulkUpsertRequest{}, errs.Newf(
				errs.KindValidationFailed, "entry bulk upsert requires %s", required,
			)
		}
	}
	input := apiTypes.EntryBulkUpsertRequest{}
	if err := decodeEntryJSONMember(members["environment_id"], &input.EnvironmentID); err != nil {
		return apiTypes.EntryBulkUpsertRequest{}, err
	}
	if err := decodeEntryJSONMember(members["exposure"], &input.Exposure); err != nil {
		return apiTypes.EntryBulkUpsertRequest{}, err
	}
	if err := decodeEntryJSONMember(members["secret"], &input.Secret); err != nil {
		return apiTypes.EntryBulkUpsertRequest{}, err
	}
	var rawEntries []json.RawMessage
	if err := decodeEntryJSONMember(members["entries"], &rawEntries); err != nil {
		return apiTypes.EntryBulkUpsertRequest{}, err
	}
	input.Entries = make([]apiTypes.EntryBulkItem, len(rawEntries))
	for index, raw := range rawEntries {
		item, err := decodeEntryBulkItem(raw)
		clear(raw)
		if err != nil {
			return apiTypes.EntryBulkUpsertRequest{}, err
		}
		input.Entries[index] = item
	}
	return input, nil
}

func decodeEntryBulkItem(body []byte) (apiTypes.EntryBulkItem, error) {
	members, err := decodeEntryJSONObject(body, "entry bulk item")
	if err != nil {
		return apiTypes.EntryBulkItem{}, err
	}
	defer clearEntryJSONMembers(members)
	for name := range members {
		if name != "key" && name != "value" {
			return apiTypes.EntryBulkItem{}, errs.New(
				errs.KindMalformedRequest, "entry bulk item contains an unknown member",
			)
		}
	}
	for _, required := range []string{"key", "value"} {
		if _, present := members[required]; !present {
			return apiTypes.EntryBulkItem{}, errs.Newf(
				errs.KindValidationFailed, "entry bulk item requires %s", required,
			)
		}
	}
	item := apiTypes.EntryBulkItem{}
	if err := decodeEntryJSONMember(members["key"], &item.Key); err != nil {
		return apiTypes.EntryBulkItem{}, err
	}
	if err := decodeEntryJSONMember(members["value"], &item.Value); err != nil {
		return apiTypes.EntryBulkItem{}, err
	}
	return item, nil
}
