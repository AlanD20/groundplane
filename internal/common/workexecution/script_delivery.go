package workexecution

import "github.com/AlanD20/groundplane/proto/agentpb"

// ClearScriptAssignment releases transient body and Entry plaintext after
// transport consumption or rejection. Metadata remains safe for diagnostics.
func ClearScriptAssignment(delivery *agentpb.ScriptWorkAssignment) {
	if delivery == nil {
		return
	}
	if delivery.Body != nil {
		clear(delivery.Body.Body)
		delivery.Body.Body = nil
	}
	for _, entry := range delivery.Entries {
		if entry != nil {
			clear(entry.Value)
			entry.Value = nil
		}
	}
}
