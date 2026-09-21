package handlers

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func decodeEntrySource(body []byte) (apiTypes.EntrySource, error) {
	members, err := decodeEntryJSONObject(body, "entry source")
	if err != nil {
		return apiTypes.EntrySource{}, err
	}
	defer clearEntryJSONMembers(members)
	allowed := map[string]struct{}{
		"kind": {}, "literal": {}, "secret_ref": {}, "attach_id": {}, "grant_attach_id": {}, "fact": {},
	}
	for name := range members {
		if _, ok := allowed[name]; !ok {
			return apiTypes.EntrySource{}, errs.New(
				errs.KindMalformedRequest,
				"entry source contains an unknown member",
			)
		}
	}
	if _, present := members["kind"]; !present {
		return apiTypes.EntrySource{}, errs.New(errs.KindValidationFailed, "entry source kind is required")
	}
	source := apiTypes.EntrySource{}
	for name, target := range map[string]*string{
		"kind": &source.Kind, "literal": &source.Literal, "secret_ref": &source.SecretRef,
		"attach_id": &source.AttachID, "grant_attach_id": &source.GrantAttachID, "fact": &source.Fact,
	} {
		if value, present := members[name]; present {
			if err := decodeEntryJSONMember(value, target); err != nil {
				return apiTypes.EntrySource{}, err
			}
		}
	}
	return source, nil
}
