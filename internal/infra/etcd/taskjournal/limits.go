package taskjournal

import "time"

const (
	MaximumTaskRecordBytes                 = 256 * 1024
	MaximumTaskEventBytes                  = 32 * 1024
	MaximumTaskEvents                      = 1000
	TaskRetention                          = 90 * 24 * time.Hour
	TaskMaterializationEnvironmentParam    = "materialization_environment_id"
	TaskMutationEnvironmentParam           = "mutation_environment_id"
	TaskBackingServiceCreationParam        = "backing_service_creation_service_id"
	TaskBackingServiceHealthParam          = "backing_service_health_service_id"
	TaskBackingServiceVolumeDirectoryParam = "backing_service_volume_directory"
)
