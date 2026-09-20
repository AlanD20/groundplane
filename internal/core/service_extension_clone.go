package core

func CloneServiceExtension(extension ServiceExtensionSpec) ServiceExtensionSpec {
	clone := extension
	if extension.Release != nil {
		release := *extension.Release
		clone.Release = &release
	}
	if extension.DependsOn != nil {
		clone.DependsOn = make(map[string]ServiceDependency, len(extension.DependsOn))
		for dependency, decision := range extension.DependsOn {
			decision.Phases = append([]ServiceDependencyPhase(nil), decision.Phases...)
			clone.DependsOn[dependency] = decision
		}
	}
	return clone
}
