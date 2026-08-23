package core

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

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
	if strings.TrimSpace(script.ID) == "" {
		return fmt.Errorf("script id is required")
	}
	if strings.TrimSpace(script.Name) == "" {
		return fmt.Errorf("script name is required")
	}
	if strings.TrimSpace(script.ServiceName) == "" {
		return fmt.Errorf("script service is required")
	}
	if script.Body == "" {
		return fmt.Errorf("script body is required")
	}
	if !utf8.ValidString(script.Body) {
		return fmt.Errorf("script body must be valid UTF-8")
	}
	if _, ok := scriptHooks[script.When]; !ok {
		return fmt.Errorf("script hook %q is invalid", script.When)
	}

	return nil
}
