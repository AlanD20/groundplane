package core

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const MaximumScriptBodyBytes = 65_536

// ScriptHook selects a manual run or one closed Release hook phase.
type ScriptHook string

const (
	ScriptManual       ScriptHook = "manual"
	ScriptPreDeploy    ScriptHook = "pre-deploy"
	ScriptPostDeploy   ScriptHook = "post-deploy"
	ScriptPreRollback  ScriptHook = "pre-rollback"
	ScriptPostRollback ScriptHook = "post-rollback"
	ScriptOnFailure    ScriptHook = "on-failure"
)

// Script is an Environment-scoped body associated with one real Service.
type Script struct {
	ID          string           `yaml:"id"      json:"id"` // scr_<ulid>
	Slug        string           `yaml:"slug"    json:"slug"`
	ServiceName string           `yaml:"service" json:"service"`
	Body        string           `yaml:"script"  json:"script"`
	When        ScriptHook       `yaml:"when"    json:"when"`
	Order       uint16           `yaml:"order,omitempty" json:"order,omitempty"`
	Execution   *ScriptExecution `yaml:"execution,omitempty" json:"execution,omitempty"`
}

// ScriptBefore orders already-selected hooks within one Service and phase.
func ScriptBefore(left, right Script) bool {
	if left.Order != right.Order {
		return left.Order < right.Order
	}
	return left.Slug < right.Slug
}

var scriptHooks = map[ScriptHook]struct{}{
	"manual":        {},
	"pre-deploy":    {},
	"post-deploy":   {},
	"pre-rollback":  {},
	"post-rollback": {},
	"on-failure":    {},
}

// Validate checks the durable Script contract without normalizing operator input.
func (script Script) Validate() error {
	if err := script.ValidateMetadata(); err != nil {
		return err
	}
	return ValidateScriptBody(script.Body)
}

// ValidateMetadata checks Script fields stored in the mutable primary record.
func (script Script) ValidateMetadata() error {
	if strings.TrimSpace(script.ID) == "" {
		return fmt.Errorf("script id is required")
	}
	if err := ValidateScriptLabel("script slug", script.Slug); err != nil {
		return err
	}
	if strings.TrimSpace(script.ServiceName) == "" {
		return fmt.Errorf("script service is required")
	}
	if _, ok := scriptHooks[script.When]; !ok {
		return fmt.Errorf("script hook %q is invalid", script.When)
	}
	if script.Execution != nil {
		return script.Execution.Validate()
	}
	return nil
}

// ValidateScriptBody checks one immutable Script body generation.
func ValidateScriptBody(body string) error {
	if strings.TrimSpace(body) == "" {
		return fmt.Errorf("script body must not be blank")
	}
	if !utf8.ValidString(body) || strings.ContainsRune(body, '\x00') {
		return fmt.Errorf("script body must be valid UTF-8 without NUL")
	}
	if len(body) > MaximumScriptBodyBytes {
		return fmt.Errorf("script body exceeds %d bytes", MaximumScriptBodyBytes)
	}
	return nil
}

// ValidateScriptLabel validates the exact reconciliation-key and slug grammar
// accepted by ADR 0040. Consecutive hyphens are valid.
func ValidateScriptLabel(field, value string) error {
	if len(value) < 1 || len(value) > 63 || !scriptLabelEdge(value[0]) || !scriptLabelEdge(value[len(value)-1]) {
		return fmt.Errorf("%s is invalid", field)
	}
	for index := 1; index < len(value)-1; index++ {
		if !scriptLabelEdge(value[index]) && value[index] != '-' {
			return fmt.Errorf("%s is invalid", field)
		}
	}
	return nil
}

func scriptLabelEdge(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
}
