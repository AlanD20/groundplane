// Package databaseversion owns observed Restore target identity and versions.
package databaseversion

import (
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Target struct {
	Family             string `json:"family"`
	ServerVersion      string `json:"server_version"`
	RestoreToolVersion string `json:"restore_tool_version"`
	ContainerID        string `json:"container_id"`
	ImageID            string `json:"image_id"`
}

func (target Target) Validate() error {
	container, err := hex.DecodeString(target.ContainerID)
	image, imageErr := hex.DecodeString(strings.TrimPrefix(target.ImageID, "sha256:"))
	if err != nil || imageErr != nil || len(container) != 32 || len(image) != 32 ||
		hex.EncodeToString(container) != target.ContainerID || "sha256:"+hex.EncodeToString(image) != target.ImageID {
		return invalid()
	}
	switch target.Family {
	case "postgres":
		server, err := postgres16protocol.ParseServerVersion(target.ServerVersion)
		tool, toolErr := postgres16protocol.ParseServerVersion(target.RestoreToolVersion)
		if err != nil || toolErr != nil || server != target.ServerVersion || tool != target.RestoreToolVersion {
			return invalid()
		}
	case "mysql":
		if mysql84protocol.ValidateObservedServerVersion(target.ServerVersion) != nil ||
			mysql84protocol.ValidateObservedToolVersion("mysql", target.RestoreToolVersion) != nil {
			return invalid()
		}
	default:
		return invalid()
	}
	return nil
}

func FromWire(value *agentpb.DatabaseVersionObservation) (Target, error) {
	if value == nil || len(value.ProtoReflect().GetUnknown()) != 0 {
		return Target{}, invalid()
	}
	target := Target{
		Family:             value.Family,
		ServerVersion:      value.ServerVersion,
		RestoreToolVersion: value.RestoreToolVersion,
		ContainerID:        value.ContainerId,
		ImageID:            value.ImageId,
	}
	return target, target.Validate()
}

func (target Target) Wire() *agentpb.DatabaseVersionObservation {
	return &agentpb.DatabaseVersionObservation{Family: target.Family, ServerVersion: target.ServerVersion,
		RestoreToolVersion: target.RestoreToolVersion, ContainerId: target.ContainerID, ImageId: target.ImageID}
}

// Tool versions differ in their executable prefix, not their version number.
func MySQLToolNumber(value string) string {
	fields := strings.Fields(value)
	for index, field := range fields {
		if field == "Ver" && index+1 < len(fields) {
			return fields[index+1]
		}
	}
	return ""
}

func invalid() error {
	return errs.New(errs.KindValidationFailed, "database target version evidence is invalid")
}
