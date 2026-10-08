// Package backupmysql owns the MySQL logical artifact evidence model.
package backupmysql

import (
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const Format = mysql84protocol.ArtifactFormat

type ArchiveEvidence struct {
	SourceServerVersion    string `json:"source_server_version"`
	BackupToolVersion      string `json:"backup_tool_version"`
	ArtifactFormat         string `json:"artifact_format"`
	AdapterContractVersion uint32 `json:"adapter_contract_version"`
}

func (evidence ArchiveEvidence) Validate() error {
	if mysql84protocol.ValidateObservedServerVersion(evidence.SourceServerVersion) != nil ||
		mysql84protocol.ValidateObservedToolVersion("mysqldump", evidence.BackupToolVersion) != nil ||
		evidence.ArtifactFormat != mysql84protocol.ArtifactFormat ||
		evidence.AdapterContractVersion != mysql84protocol.AdapterContractVersion {
		return errs.New(errs.KindValidationFailed, "MySQL backup archive evidence is invalid")
	}
	return nil
}

func FromWire(value *agentpb.BackupMySQLArchiveEvidence) (ArchiveEvidence, error) {
	if value == nil {
		return ArchiveEvidence{}, errs.New(errs.KindValidationFailed, "MySQL backup archive evidence is required")
	}
	evidence := ArchiveEvidence{SourceServerVersion: value.SourceServerVersion,
		BackupToolVersion: value.BackupToolVersion, ArtifactFormat: value.ArtifactFormat,
		AdapterContractVersion: value.AdapterContractVersion}
	return evidence, evidence.Validate()
}

func (evidence ArchiveEvidence) Wire() (*agentpb.BackupMySQLArchiveEvidence, error) {
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	return &agentpb.BackupMySQLArchiveEvidence{SourceServerVersion: evidence.SourceServerVersion,
		BackupToolVersion: evidence.BackupToolVersion, ArtifactFormat: evidence.ArtifactFormat,
		AdapterContractVersion: evidence.AdapterContractVersion}, nil
}
