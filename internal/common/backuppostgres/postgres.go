// Package backuppostgres owns the opaque postgres-custom-v1 artifact model.
package backuppostgres

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	Format                = "postgres-custom-v1"
	PGDumpMajor    uint32 = 16
	AdapterVersion uint32 = 1
)

type ArchiveEvidence struct {
	PGDumpMajor            uint32
	AdapterContractVersion uint32
}

type ArtifactEvidence struct {
	Source  backupformat.Evidence
	Archive ArchiveEvidence
}

func (evidence ArchiveEvidence) Validate() error {
	if evidence.PGDumpMajor != PGDumpMajor {
		return errs.New(errs.KindValidationFailed, "postgres backup pg_dump major must be 16")
	}
	if evidence.AdapterContractVersion != AdapterVersion {
		return errs.New(errs.KindValidationFailed, "postgres backup adapter contract version must be 1")
	}
	return nil
}

func (evidence ArtifactEvidence) Validate() error {
	if err := evidence.Archive.Validate(); err != nil {
		return err
	}
	return evidence.Source.Validate(backupformat.MaxStoredBytes)
}

// VerifySource requires the exact opaque pg_dump bytes, including exact EOF.
func VerifySource(ctx context.Context, source io.Reader, evidence ArtifactEvidence) error {
	if err := evidence.Validate(); err != nil {
		return err
	}
	return backupformat.Verify(ctx, source, evidence.Source, backupformat.MaxStoredBytes)
}
