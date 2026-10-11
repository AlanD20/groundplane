package taskprogress

import "github.com/AlanD20/groundplane/pkg/errs"

// ControllerOperation names an observed native action, never an invented Agent
// step or user-supplied log message. Receipt timestamps belong to the journal.
type ControllerOperation struct {
	Stage string `json:"stage"`
}

var controllerOperationNames = map[string]string{
	"agent_inspect":               "Inspect Agent registration",
	"agent_enroll":                "Enroll and start Agent",
	"agent_reconcile":             "Reconcile Agent runtime",
	"agent_remove":                "Remove Agent",
	"image_fetch":                 "Fetch workload image",
	"image_remove":                "Remove workload image",
	"connection_check":            "Check connection",
	"software_checkpoint_check":   "Verify previously published software artifacts",
	"software_source_download":    "Download and verify selected source archive",
	"software_controller_build":   "Build Controller bundle in container",
	"software_agent_build":        "Build Agent image in container",
	"software_release_import":     "Download and verify published release artifacts",
	"software_agent_publish":      "Publish Agent image to managed registry",
	"software_controller_publish": "Publish Controller bundle to managed registry",
	"software_output_check":       "Verify prepared artifacts in managed registry",
	"controller_update_prepare":   "Check Controller update and drain Agent",
	"controller_update_stage":     "Stage Controller candidate and handoff journal",
	"controller_handoff":          "Launch Controller process handoff",
	"controller_wait_restart":     "Wait for Controller process restart",
	"controller_candidate_check":  "Check candidate Controller readiness",
	"controller_final_check":      "Confirm Controller readiness after Agent reconciliation",
	"controller_restore_check":    "Check restored Controller readiness",
	"agent_update":                "Reconcile candidate Agent and await authenticated Ready",
	"agent_restore":               "Restore previous Agent and await authenticated Ready",
	"agent_ready_check":           "Confirm previously qualified Agent is ready",
	"etcd_apply":                  "Apply etcd configuration",
	"backup_key_rotate":           "Rotate backup encryption key",
	"hierarchy_remove":            "Remove resource hierarchy",
	"backing_zone_remove":         "Remove shared backing Zone",
	"runner_create":               "Create and register Runner",
	"runner_remove":               "Remove Runner",
}

func (operation *ControllerOperation) Validate() error {
	if operation == nil {
		return nil
	}
	if _, ok := controllerOperationNames[operation.Stage]; !ok {
		return errs.New(errs.KindValidationFailed, "Controller operation stage is invalid")
	}
	return nil
}

func (operation *ControllerOperation) Name() string {
	if operation == nil {
		return ""
	}
	return controllerOperationNames[operation.Stage]
}

func CloneControllerOperation(operation *ControllerOperation) *ControllerOperation {
	if operation == nil {
		return nil
	}
	owned := *operation
	return &owned
}
