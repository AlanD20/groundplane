// Package backuppostgres owns the PostgreSQL custom archive evidence model.
package backuppostgres

import (
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ArchiveEvidence struct {
	PGDumpMajor            uint32 `json:"pg_dump_major"`
	AdapterContractVersion uint32 `json:"adapter_contract_version"`
	SourceServerVersion    string `json:"source_server_version"`
	BackupToolVersion      string `json:"backup_tool_version"`
}

func (evidence ArchiveEvidence) Validate() error {
	if evidence.PGDumpMajor != postgres16protocol.PostgreSQLMajor ||
		evidence.AdapterContractVersion != postgres16protocol.AdapterContractVersion ||
		!validObservedVersion(evidence.SourceServerVersion) || !validObservedVersion(evidence.BackupToolVersion) {
		return errs.New(errs.KindValidationFailed, "PostgreSQL backup archive evidence is invalid")
	}
	return nil
}

func FromWire(value *agentpb.BackupPostgresArchiveEvidence) (ArchiveEvidence, error) {
	if value == nil {
		return ArchiveEvidence{}, errs.New(errs.KindValidationFailed, "PostgreSQL backup archive evidence is required")
	}
	evidence := ArchiveEvidence{PGDumpMajor: value.PgDumpMajor,
		AdapterContractVersion: value.AdapterContractVersion,
		SourceServerVersion:    value.SourceServerVersion, BackupToolVersion: value.BackupToolVersion}
	return evidence, evidence.Validate()
}

func (evidence ArchiveEvidence) Wire() (*agentpb.BackupPostgresArchiveEvidence, error) {
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	return &agentpb.BackupPostgresArchiveEvidence{PgDumpMajor: evidence.PGDumpMajor,
		AdapterContractVersion: evidence.AdapterContractVersion,
		SourceServerVersion:    evidence.SourceServerVersion, BackupToolVersion: evidence.BackupToolVersion}, nil
}

func validObservedVersion(value string) bool {
	parsed, err := postgres16protocol.ParseServerVersion(value)
	return err == nil && parsed == value
}
