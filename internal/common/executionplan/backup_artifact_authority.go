package executionplan

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// CaptureBackupArtifactAuthority owns a validated resource input without binding
// artifact transport to a Task journal or execution envelope. The executor owns
// assignment authentication before supplying this input.
func CaptureBackupArtifactAuthority(value *agentpb.BackupCaptureAuthority) (*agentpb.BackupCaptureAuthority, error) {
	if value == nil {
		return nil, errs.New(errs.KindValidationFailed, "backup capture authority is missing")
	}
	owned := proto.Clone(value).(*agentpb.BackupCaptureAuthority)
	if err := RejectUnknown(owned); err != nil {
		return nil, err
	}
	if !validBackupCaptureAuthority(owned) {
		return nil, errs.New(errs.KindValidationFailed, "backup capture authority is invalid")
	}
	return owned, nil
}

// CaptureRestoreArtifactAuthority freezes the selected object and destination;
// downloading it never resolves current desired state or selects a newer object.
func CaptureRestoreArtifactAuthority(value *agentpb.BackupRestoreAuthority) (*agentpb.BackupRestoreAuthority, error) {
	if value == nil {
		return nil, errs.New(errs.KindValidationFailed, "backup restore authority is missing")
	}
	owned := proto.Clone(value).(*agentpb.BackupRestoreAuthority)
	if err := RejectUnknown(owned); err != nil {
		return nil, err
	}
	if !validBackupRestoreAuthority(owned) {
		return nil, errs.New(errs.KindValidationFailed, "backup restore authority is invalid")
	}
	return owned, nil
}
