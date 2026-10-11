package catalog

// ContainerConfigActivation selects a compiled executor, not an operator-authored
// command or a dynamically loaded provider. HTTP discovery needs full active
// configuration proof; a command's successful exit cannot substitute for it.
type ContainerConfigActivation string

const (
	ContainerConfigActivationCommand       ContainerConfigActivation = "command"
	ContainerConfigActivationHTTPDiscovery ContainerConfigActivation = "http-discovery"
)

func validContainerActivation(activation ContainerConfigActivation, arguments []string) bool {
	switch activation {
	case ContainerConfigActivationCommand:
		return validContainerCommand(arguments)
	case ContainerConfigActivationHTTPDiscovery:
		return len(arguments) == 0
	default:
		return false
	}
}
