package scripts

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strconv"
)

const (
	scriptSetRecordPrefix          = "/v1/records/script-sets/"
	scriptLocatorPrefix            = "/v1/indexes/scripts/by-id/"
	scriptEnvironmentLocatorPrefix = "/v1/indexes/scripts/by-environment/"
)

func ScriptLocatorKey(id string) string { return scriptLocatorPrefix + id }

func ScriptEnvironmentLocatorPrefixFor(environmentID string) string {
	return scriptEnvironmentLocatorPrefix + environmentID + "/"
}

func ScriptEnvironmentLocatorKey(environmentID, id string) string {
	return ScriptEnvironmentLocatorPrefixFor(environmentID) + id
}

func ScriptSetActiveKey(environmentID string) string {
	return scriptSetRecordPrefix + environmentID + "/active"
}

func ScriptSetEnvironmentPrefix(environmentID string) string {
	return scriptSetRecordPrefix + environmentID + "/"
}

func ScriptSetGenerationPrefix(environmentID, generationID string) string {
	return ScriptSetEnvironmentPrefix(environmentID) + "generations/" + generationID + "/"
}

func ScriptSetScriptKey(environmentID, generationID, id string) string {
	return ScriptSetGenerationPrefix(environmentID, generationID) + "scripts/" + id
}

func ScriptSetBodyGenerationPrefix(environmentID, generationID, id string) string {
	return ScriptSetGenerationPrefix(environmentID, generationID) + "bodies/" + id + "/"
}

func ScriptSetBodyGenerationKey(environmentID, generationID, id string, generation uint64) string {
	return ScriptSetBodyGenerationPrefix(environmentID, generationID, id) + strconv.FormatUint(generation, 10)
}

func ScriptSetOwnerPrefix(environmentID, generationID string) string {
	return ScriptSetGenerationPrefix(environmentID, generationID) + "owners/"
}

func ScriptSetOwnerKey(environmentID, generationID, scriptID string) string {
	return ScriptSetOwnerPrefix(environmentID, generationID) + scriptID
}

func ScriptSetSlugKey(environmentID, generationID, slug string) string {
	return ScriptSetGenerationPrefix(environmentID, generationID) + "slugs/" + recordcodec.EncodeKeySegment(slug)
}
