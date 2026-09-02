// Package taskcontract owns closed durable Task parameter names shared by
// Controller capabilities and the execution-plan resolver.
package taskcontract

const (
	EnvironmentCreateVolumeDirectoryParam = "expected_volume_dir"
	EnvironmentBlueprintArtifactParam     = "compose_artifact_id"
	EnvironmentRemoveVolumeDirectoryParam = "remove_volume_dir"
	MaximumBlueprintPostDeployHooks       = 16
)

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
