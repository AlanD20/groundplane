package servicelifecyclework

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

// The captured procedure cannot outlive a change to the applied runtime while
// admission is being prepared. These comparisons join resource and Task writes.
func runtimeSourceConditions(input Input) []keyvalue.Condition {
	runtime := input.Runtime
	if runtime == nil {
		conditions := []keyvalue.Condition{{Key: serviceruntimerecord.Key(input.ServiceID)},
			{Key: releases.ReleaseProjectionKey(input.ServiceID)}}
		if input.Scope.TenantID == "" {
			conditions = append(conditions, keyvalue.Condition{
				Key: environmentprojection.EnvironmentComposeProjectionStorageKey(input.Scope.EnvironmentID),
			})
		}
		return conditions
	}
	conditions := []keyvalue.Condition{
		{Key: serviceruntimerecord.Key(input.ServiceID), ModRevision: runtime.AcknowledgedRuntimeRevision},
	}
	if runtime.NativeBacking != nil {
		conditions = append(conditions, keyvalue.Condition{
			Key:         environmentprojection.EnvironmentComposeProjectionStorageKey(input.Scope.EnvironmentID),
			ModRevision: runtime.NativeBackingRevision,
		})
	}
	if release := runtime.Release; release != nil {
		conditions = append(
			conditions,
			keyvalue.Condition{
				Key:         releases.ReleaseProjectionKey(input.ServiceID),
				ModRevision: release.ProjectionRevision,
			},
			keyvalue.Condition{
				Key:         releases.ReleaseIntentStagingKey("", release.ServingReleaseID),
				ModRevision: release.IntentRevision,
			},
			keyvalue.Condition{
				Key:         releases.ReleaseRenderInputStagingKey("", release.ServingReleaseID),
				ModRevision: release.RenderRevision,
			},
		)
		if release.RetainedPrior != nil {
			conditions = append(
				conditions,
				keyvalue.Condition{
					Key:         releases.ReleaseRenderInputStagingKey("", release.PriorServingReleaseID),
					ModRevision: release.RetainedPriorRenderRevision,
				},
			)
		}
	}
	return conditions
}
