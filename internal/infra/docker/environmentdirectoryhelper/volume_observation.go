package environmentdirectoryhelper

type ManagedVolumeInspectRequest struct {
	VolumeRoot string
	VolumeDir  string
	VolumeID   string
	ComposeKey string
}

// Ready means the leaf is a real directory with no unfinished creation marker.
// Its uid/mode may belong to the workload. Parent ownership remains GP-owned.
type ManagedVolumeObservation struct {
	Ready  bool
	Absent bool
}
