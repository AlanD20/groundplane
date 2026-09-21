package entries

import (
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
)

const entryOwnerPrefix = "/v1/indexes/entries/by-owner/environment/"

const BlueprintEntryEnvironmentPrefix = "/v1/indexes/entries/blueprint-environment/"

// EntryValueGeneration is the closed atomic value input for an Entry mutation.
type EntryValueGeneration struct {
	Plain  *entryvalues.PlainGeneration
	Secret *entryvalues.SecretGeneration
}
