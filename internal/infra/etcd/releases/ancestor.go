package releases

import "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"

func AncestorTombstone(tenantID, projectID string) string {
	if tenantID == "" {
		return deletions.TombstoneKey("project", projectID)
	}
	return deletions.TombstoneKey("tenant", tenantID)
}
