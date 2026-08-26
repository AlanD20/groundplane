// Package taskcontract owns closed durable Task parameter names shared by
// Controller capabilities and the execution-plan resolver.
package taskcontract

const (
	EnvironmentCreateVolumeDirectoryParam = "expected_volume_dir"
	EnvironmentBlueprintArtifactParam     = "compose_artifact_id"
	EnvironmentRemoveVolumeDirectoryParam = "remove_volume_dir"
)
