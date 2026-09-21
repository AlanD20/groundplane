package etcd

import (
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"testing"
)

// Rationale: a completed Environment-create Task remains immutable journal
// evidence after its target hierarchy is deleted and must stay listable.
func TestDecodeTaskRecordAcceptsCompletedEnvironmentCreate(t *testing.T) {
	value := []byte(
		`{"schema":1,"kind":"task","data":{"id":"task_01M14NETH1HW2DGVSX2SSVNH4R","operation_id":"op_01M14NETH2ADH05NJTEDRAZK0X","idempotency_key":"01M14NEV1K2FRBAZQWSDXF0YJY","owner":{"workspace_type":"tenant","tenant_id":"tnt_01M14NETG2MPP2QD34D6XPQJ8P","project_id":"prj_01M14NETGKQP24JP5WBFKBK1B2","environment_id":"env_01M14NETH1HW2DGVSX2T7B64QN"},"actor":"operator","executor":"agent","plan_id":"plan_01M14NETH1HW2DGVSX31MGKMEN","plan_hash":"1e345e8a0bfb79134276b336dc44280208d47b445f4500bb817e54902703e324","render_generation":1,"type":"create","target":"env_01M14NETH1HW2DGVSX2T7B64QN","params":{"expected_volume_dir":"/var/lib/groundplane/vol/tnt_01M14NETG2MPP2QD34D6XPQJ8P/prj_01M14NETGKQP24JP5WBFKBK1B2/env_01M14NETH1HW2DGVSX2T7B64QN"},"steps":[{"kind":"operation","id":"step_01M14NETH1HW2DGVSX31P7NWKM"}],"timeout_seconds":120,"status":"completed","result":{"kind":"environment_directory","exit_code":0,"diagnostic":"none","reconciliation_required":false},"terminal_assignment":{"assignment_id":"asgn_01M14NEV6A68TRVKQPS5NR69TN","agent_id":"agt_01M13ZGQNA892Z1MXGRR7HGRJ7","agent_generation":13},"next_event_sequence":3,"event_count":2,"created_at":"2026-08-28T17:07:40.705955952Z","updated_at":"2026-08-28T17:07:41.490530633Z","started_at":"2026-08-28T17:07:41.385441161Z","finished_at":"2026-08-28T17:07:41.490530633Z","retain_until":"2026-11-26T17:07:41.490530633Z","idempotency_marker":{"scope_kind":"project","scope_id":"prj_01M14NETGKQP24JP5WBFKBK1B2","method":"POST","route":"/environments","key":"01M14NEV1K2FRBAZQWSDXF0YJY"}}}`,
	)

	if _, err := DecodeTaskRecord(value); err != nil {
		data, envelopeErr := testrecordcodec.Decode[taskRecordData](value, "task")
		record, timestampErr := taskRecordFromData(data)
		t.Fatalf(
			"decodeTaskRecord() error = %v; envelope = %v; timestamps = %v; validation = %v",
			err,
			envelopeErr,
			timestampErr, ValidateTaskRecord(record),
		)
	}
}
