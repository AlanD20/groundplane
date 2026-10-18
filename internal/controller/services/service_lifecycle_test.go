package services

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

func TestServiceInComposeProjectionUsesDesiredAndAddressedGeneratedServices(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 23, 13, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	desiredID := ids.NewAt(ids.KindService, at, 2)
	generatedID := ids.NewAt(ids.KindService, at, 3)
	transitionalID := ids.NewAt(ids.KindService, at, 4)
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: environmentID, Desired: core.Service{ID: desiredID, Name: "api"},
		}},
		Components: []testcomponents.Record{{
			Runtime: testcomponents.RuntimeRecord{GeneratedServices: []string{generatedID}},
		}},
	}
	if !serviceInComposeProjection(projection, desiredID) || !serviceInComposeProjection(projection, generatedID) ||
		serviceInComposeProjection(projection, transitionalID) {
		t.Fatalf("service membership did not follow authoritative desired topology")
	}
}
