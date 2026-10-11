package rollingrelease

import "github.com/AlanD20/groundplane/pkg/errs"

// RoutingName is either a sealed singleton alias or an observed scaled
// container name. It never synthesizes a Docker name from a Compose ordinal.
func (replica Replica) RoutingName() string {
	if replica.ContainerID != "" {
		return replica.ContainerName
	}
	return replica.ComposeName
}

func (replica Replica) validateContainerIdentity() error {
	if replica.ContainerID == "" && replica.ContainerName == "" && replica.ComposeReplica == 0 {
		return nil
	}
	if len(replica.ContainerID) != 64 || !validContainerDNSName(replica.ContainerName) ||
		replica.ComposeReplica == 0 || replica.ComposeReplica > MaximumReplicas {
		return errs.New(
			errs.KindValidationFailed,
			"scaled predecessor requires its full container id, name and Compose replica number",
		)
	}
	for _, character := range replica.ContainerID {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return errs.New(errs.KindValidationFailed, "scaled predecessor container id is invalid")
		}
	}
	return nil
}

func validContainerDNSName(value string) bool {
	if len(value) == 0 || len(value) > 253 {
		return false
	}
	for index, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' &&
				character <= '9' || index > 0 && (character == '-' || character == '_' || character == '.') {
			continue
		}
		return false
	}
	return true
}
