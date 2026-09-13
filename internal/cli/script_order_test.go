package cli

import (
	"net/http"
	"testing"
)

// QA: SCRIPT-01, UI-01, UI-03; local CLI request encoding only, not runtime hook ordering or surface parity.
// Rationale: CLI order authoring must reach the same API field and allow an
// explicit zero patch; the generated-client conversion must not discard it.
func TestScriptAddAndEditCarryNumericOrder(t *testing.T) {
	const environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const serviceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const scriptID = "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const response = `{"id":"` + scriptID + `","environment_id":"` + environmentID +
		`","service_id":"` + serviceID + `","slug":"migrate","service":"api","script":"echo migrate",` +
		`"when":"pre-deploy","order":65535,"origin":"api","active_generation":1,"execution":{"mode":"inherited"}}`
	create := exactRequestServer(t, http.MethodPost, "/api/v1/scripts",
		`{"environment_id":"`+environmentID+`","order":65535,"script":"echo migrate","service_id":"`+serviceID+
			`","slug":"migrate","when":"pre-deploy"}`, http.StatusCreated, response)
	defer create.Close()
	executeNoun(
		t,
		newScriptCmd(),
		create.URL,
		Scope{Environment: environmentID, AsID: true},
		"add",
		"migrate",
		"--service",
		serviceID,
		"--script",
		"echo migrate",
		"--when",
		"pre-deploy",
		"--order",
		"65535",
	)
	edit := exactRequestServer(t, http.MethodPatch, "/api/v1/scripts/"+scriptID,
		`{"order":0}`, http.StatusOK, response)
	defer edit.Close()
	executeNoun(t, newScriptCmd(), edit.URL, Scope{AsID: true}, "edit", scriptID, "--order", "0")
}
