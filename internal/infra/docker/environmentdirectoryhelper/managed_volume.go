// Package environmentdirectoryhelper owns descriptor-relative Environment and
// managed Volume directory operations used by native host helpers.
package environmentdirectoryhelper

const VolumeRootEnv = "GROUNDPLANE_VOLUME_ROOT"

type ManagedVolume struct {
	ID  string
	Key string
}

type ManagedVolumeEnsureRequest struct {
	TaskID       string
	OperationID  string
	IntentSHA256 []byte
	VolumeRoot   string
	VolumeDir    string
	Volumes      []ManagedVolume
}

type ManagedVolumeDirectoryRemoveRequest struct {
	TaskID       string
	OperationID  string
	IntentSHA256 []byte
	Cursor       []byte
	VolumeRoot   string
	VolumeDir    string
	VolumeID     string
	ComposeKey   string
}

type ManagedVolumeDirectoryRemoveResult struct {
	NextCursor     []byte
	MutationCount  uint32
	Complete       bool
	ResponseSHA256 []byte
}
