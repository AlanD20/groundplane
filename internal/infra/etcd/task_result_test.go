package etcd

import (
	"reflect"
	"testing"
)

func TestTaskResultRecordRoundTripsAsBoundedSummary(t *testing.T) {
	record := validTaskRecord(taskJournalTime())
	startedAt := record.CreatedAt.Add(1)
	terminalAt := record.CreatedAt.Add(2)
	record.Status = TaskStatusCompleted
	record.StartedAt = &startedAt
	record.TerminalAt = &terminalAt
	retainUntil := terminalAt.Add(TaskRetention)
	record.RetainUntil = &retainUntil
	result := completedComposeTaskResult()
	result.Projects = []TaskObservedProjectSummary{{
		ProjectName: "gp-platform", ObservedAt: terminalAt,
		ContainerCount: 3, NetworkCount: 2, VolumeCount: 1,
	}}
	record.Result = &result

	encoded, err := encodeTaskRecord(record)
	if err != nil {
		t.Fatalf("encodeTaskRecord() error = %v", err)
	}
	decoded, err := decodeTaskRecord(encoded)
	if err != nil {
		t.Fatalf("decodeTaskRecord() error = %v", err)
	}
	if !reflect.DeepEqual(decoded.Result, record.Result) {
		t.Fatalf("decoded result = %#v, want %#v", decoded.Result, record.Result)
	}
}

func completedComposeTaskResult() TaskResultRecord {
	return TaskResultRecord{
		Kind:       TaskResultCompose,
		Diagnostic: TaskResultDiagnosticNone,
	}
}
