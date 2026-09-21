package etcd

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testplatformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
)

func TestDisabledComponentObservationRetainsManagedFileDigest(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 10, 0, 0, 0, time.UTC)
	record := testplatformcomponents.ComponentObservationRecord{
		ComponentID:       ids.NewAt(ids.KindComponent, now, 1),
		ServiceID:         ids.NewAt(ids.KindService, now, 2),
		PlanID:            ids.NewAt(ids.KindPlan, now, 3),
		ComposeArtifactID: ids.NewAt(ids.KindConfig, now, 4),
		DesiredGeneration: 1, RenderGeneration: 1,
		AgentID: ids.NewAt(ids.KindAgent, now, 5), AgentGeneration: 1,
		BaselineGeneration: 1, OwnershipGeneration: 1,
		CorefileSHA256: strings.Repeat("a", 64), ObservedAt: now,
		TaskID: ids.NewAt(ids.KindTask, now, 6), StepID: ids.NewAt(ids.KindStep, now, 7), Revision: 1,
	}
	if err := testplatformcomponents.ValidateComponentObservation(record); err != nil {
		t.Fatalf("validateComponentObservation(disabled retained digest) error = %v", err)
	}
	record.CorefileSHA256 = "not-a-digest"
	if err := testplatformcomponents.ValidateComponentObservation(record); err == nil {
		t.Fatal("validateComponentObservation() accepted an invalid retained digest")
	}
}
