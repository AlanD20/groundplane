package volume

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"sort"
	"strings"
)

func stableIDFromTask(kind ids.Kind, taskID string) string {
	if ids.Validate(ids.KindTask, taskID) != nil || len(taskID) <= len("task_") {
		return ""
	}
	return string(kind) + "_" + strings.TrimPrefix(taskID, "task_")
}

func volumeMutationConsumerIDs(projection projectionrecord.EnvironmentComposeProjection, volumeID string) []string {
	seen := make(map[string]struct{})
	for _, mount := range projection.VolumeMounts {
		if mount.VolumeID == volumeID {
			seen[mount.ServiceID] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for serviceID := range seen {
		result = append(result, serviceID)
	}
	sort.Strings(result)
	return result
}
