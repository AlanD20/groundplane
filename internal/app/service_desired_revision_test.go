package app

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestServiceZoneNetworkIdentitiesAddsResolvedZone(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	zoneID := ids.NewAt(ids.KindNetwork, now, 1)
	result, err := serviceZoneNetworkIdentities(nil, []etcd.Versioned[etcd.ZoneRecord]{{
		Record: etcd.ZoneRecord{Desired: core.Zone{ID: zoneID, Name: "frontend"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0].ID != zoneID || result[0].Name != "frontend" {
		t.Fatalf("network identities = %#v", result)
	}
}
