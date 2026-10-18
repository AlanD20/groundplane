package taskinspection

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/etcdconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
)

var resourceKinds = map[ids.Kind]string{
	ids.KindTenant: "tenant", ids.KindProject: "project", ids.KindEnvironment: "environment",
	ids.KindService: "service", ids.KindVolume: "volume", ids.KindAttach: "attach",
	ids.KindEnvEntry: "entry",
	ids.KindRoute:    "route", ids.KindDNSRecord: "dns", ids.KindSecret: "secret",
	ids.KindConnector: "connector", ids.KindRunner: "runner", ids.KindScript: "script",
	ids.KindBackupSource: "backup_source", ids.KindRecoveryPoint: "recovery_point",
	ids.KindAgent: "agent", ids.KindHost: "host", ids.KindNetwork: "zone",
	ids.KindBackingService: "backing_service", ids.KindReleaseGroup: "release_group",
	ids.KindComponent: "component", ids.KindConfig: "configuration",
}

// Resource identity is immutable even when labels change or definitions are
// deleted. Classification requires a validated ID, never a name or log match.
func resourceKind(id string) string {
	switch id {
	case controllersoftware.TargetID():
		return "controller"
	case etcdconfiguration.TargetID():
		return "etcd"
	case softwarepreparation.TargetID():
		return "software"
	}
	if workloadimage.LocalIDValid(id) {
		return "image"
	}
	for kind, name := range resourceKinds {
		if ids.Validate(kind, id) == nil {
			return name
		}
	}
	return ""
}

func validResourceFilter(value string) bool {
	if value == "" || value == "image" || value == "controller" || value == "etcd" || value == "software" {
		return true
	}
	for _, name := range resourceKinds {
		if name == value {
			return true
		}
	}
	return false
}
