// Package taskcontract owns closed durable Task parameter names shared by
// Controller capabilities and the execution-plan resolver.
package taskcontract

const (
	EnvironmentCreateVolumeDirectoryParam = "expected_volume_dir"
	EnvironmentBlueprintArtifactParam     = "compose_artifact_id"
	EnvironmentBlueprintProcedureParam    = "blueprint_compose_procedure"
	EnvironmentRemoveVolumeDirectoryParam = "remove_volume_dir"
	MaximumBlueprintPostDeployHooks       = 16
)

type BlueprintComposeProcedure string

const (
	BlueprintComposeProcedureNone              BlueprintComposeProcedure = "none"
	BlueprintComposeProcedureFullReconcile     BlueprintComposeProcedure = "full-reconcile"
	BlueprintComposeProcedureCandidateReleases BlueprintComposeProcedure = "candidate-releases"
)

func ParseBlueprintComposeProcedure(value string) (BlueprintComposeProcedure, bool) {
	procedure := BlueprintComposeProcedure(value)
	switch procedure {
	case BlueprintComposeProcedureNone,
		BlueprintComposeProcedureFullReconcile,
		BlueprintComposeProcedureCandidateReleases:
		return procedure, true
	default:
		return "", false
	}
}

func BlueprintReleaseProcedureStepCount(memberCount, hookCount int) (int, bool) {
	if memberCount <= 0 || hookCount < 0 || hookCount > MaximumBlueprintPostDeployHooks {
		return 0, false
	}
	maxInt := int(^uint(0) >> 1)
	if memberCount > (maxInt-hookCount)/2 {
		return 0, false
	}
	return memberCount*2 + hookCount, true
}
