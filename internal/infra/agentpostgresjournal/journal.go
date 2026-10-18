// Package agentpostgresjournal owns the Agent's private crash-durable record
// that a managed database helper execution still requires authoritative retirement.
package agentpostgresjournal

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/oklog/ulid/v2"
)

const (
	rootPath       = agentprotocol.StatePath + "/database-executions"
	maximumMarkers = 32
)

type IDs struct {
	Task        string
	ExecutionID string
	Point       string
}

func (value IDs) validate() error {
	if ids.Validate(ids.KindTask, value.Task) != nil ||
		ids.Validate(ids.KindRecoveryPoint, value.Point) != nil {
		return invalid("database execution journal IDs are invalid")
	}
	execution, err := ulid.ParseStrict(value.ExecutionID)
	if err != nil || execution.String() != value.ExecutionID {
		return invalid("database execution journal source identity is invalid")
	}
	return nil
}

func (value IDs) name() string {
	return value.Task + "." + value.ExecutionID + "." + value.Point
}

func parseName(name string) (IDs, error) {
	parts := strings.Split(name, ".")
	if len(parts) != 3 {
		return IDs{}, invalid("database execution journal marker name is invalid")
	}
	value := IDs{Task: parts[0], ExecutionID: parts[1], Point: parts[2]}
	if value.validate() != nil || value.name() != name {
		return IDs{}, invalid("database execution journal marker name is invalid")
	}
	return value, nil
}

func invalid(message string) error {
	return errs.New(errs.KindValidationFailed, message)
}

func conflict(message string) error {
	return errs.New(errs.KindStateConflict, message)
}

func storage(err error) error {
	if err == nil {
		return nil
	}
	return errs.Wrap(errs.KindStorageUnavailable, err)
}
