package taskjournal

import ()

// TaskEventCheckpoint retains bounded step progress and mutation facts, not a
// second history. Its identity is the replay watermark of evicted events.
type TaskEventCheckpoint struct {
	Identity       TaskEventIdentity `json:"identity"`
	Sequence       uint64            `json:"sequence"`
	PayloadSHA256  string            `json:"payload_sha256"`
	State          TaskEventState    `json:"state"`
	Running        bool              `json:"running"`
	Completed      bool              `json:"completed"`
	EffectPossible bool              `json:"effect_possible"`
}
