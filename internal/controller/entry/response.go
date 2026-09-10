package entry

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Response projects persisted metadata, including the immutable authoring key
// needed to resolve Script grants. It does not resolve or reveal Entry values.
func Response(record etcd.EntryRecord) apiTypes.Entry {
	entry := record.Entry
	response := apiTypes.Entry{
		ID: entry.ID, Type: string(entry.Kind), Key: entry.Key, Path: entry.Path,
		Source: apiTypes.EntrySource{
			Kind: string(entry.Source.Kind), Literal: entry.Source.Literal, SecretRef: entry.Source.SecretRef,
		},
		Exposure: append([]string(nil), entry.Exposure...), Secret: entry.Secret,
		ReconciliationKey: record.BlueprintKey,
	}
	if entry.UID != nil {
		value := int64(*entry.UID)
		response.UID = &value
	}
	if entry.GID != nil {
		value := int64(*entry.GID)
		response.GID = &value
	}
	if entry.Source.Fact != nil {
		response.Source.AttachID = entry.Source.Fact.Attach
		response.Source.GrantAttachID = entry.Source.Fact.Grant
		response.Source.Fact = entry.Source.Fact.Key
	}
	return response
}
