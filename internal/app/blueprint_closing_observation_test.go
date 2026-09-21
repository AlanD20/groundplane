package app

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Observe persisted continuation evidence independently of the repository's
// private decoder. This is used only after exercising real acknowledgement.
type integrationClosingReport struct {
	Status               taskjournal.TaskStatus       `json:"status"`
	Result               taskjournal.TaskResultRecord `json:"result"`
	ObservedAt           time.Time                    `json:"observed_at"`
	ExecutionEpoch       uint32                       `json:"execution_epoch"`
	RecoveryRecordSHA256 string                       `json:"recovery_record_sha256"`
}

func integrationBlueprintClosingReportKey(taskID string) string {
	return "/v1/records/blueprint-closing-reports/" + taskID
}

func readIntegrationClosingReport(
	t *testing.T,
	store keyvalue.Store,
	taskID string,
) (integrationClosingReport, *keyvalue.KeyValue) {
	t.Helper()
	read, err := store.Get(t.Context(), integrationBlueprintClosingReportKey(taskID))
	if err != nil || read == nil || read.Entry == nil {
		t.Fatalf("missing persisted closing report: %v", err)
	}
	var envelope struct {
		Schema int                      `json:"schema"`
		Kind   string                   `json:"kind"`
		Data   integrationClosingReport `json:"data"`
	}
	if err := json.Unmarshal(read.Entry.Value, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Schema != 1 || envelope.Kind != "blueprint-closing-report" {
		t.Fatal("unexpected persisted closing report envelope")
	}
	// Result excludes these fields from JSON; continuation authority stores them
	// alongside the result, so compare the complete persisted report.
	envelope.Data.Result.ExecutionEpoch = envelope.Data.ExecutionEpoch
	envelope.Data.Result.ReleaseRecoveryRecordSHA256 = envelope.Data.RecoveryRecordSHA256
	return envelope.Data, read.Entry
}
