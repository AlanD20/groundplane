package hierarchydeletionplanning

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

const hierarchyDeletionZoneEvidenceKind = "hierarchy_deletion_zone_evidence"

type hierarchyDeletionZoneEvidence struct {
	EnvironmentID      string    `json:"environment_id"`
	ZoneID             string    `json:"zone_id"`
	Desired            core.Zone `json:"desired"`
	ProjectionRevision int64     `json:"projection_revision"`
}

func newHierarchyDeletionZoneEvidence(
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	desired projectionrecord.EnvironmentZoneProjection,
) (hierarchyDeletionZoneEvidence, error) {
	evidence := hierarchyDeletionZoneEvidence{
		EnvironmentID: desired.EnvironmentID, ZoneID: desired.Desired.ID,
		Desired: desired.Desired, ProjectionRevision: projection.Revision,
	}
	if projection.ReadRevision <= 0 || projection.Revision <= 0 ||
		projection.Record.EnvironmentID != desired.EnvironmentID ||
		validateHierarchyDeletionZoneEvidence(evidence) != nil {
		return hierarchyDeletionZoneEvidence{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return evidence, nil
}

func validateHierarchyDeletionZoneEvidence(evidence hierarchyDeletionZoneEvidence) error {
	if evidence.ProjectionRevision <= 0 || evidence.ZoneID != evidence.Desired.ID ||
		zonerecord.ValidateRecord(zonerecord.Record{EnvironmentID: evidence.EnvironmentID, Desired: evidence.Desired}) != nil {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}

func encodeHierarchyDeletionZoneEvidence(evidence hierarchyDeletionZoneEvidence) ([]byte, error) {
	if err := validateHierarchyDeletionZoneEvidence(evidence); err != nil {
		return nil, err
	}
	return recordcodec.Encode(hierarchyDeletionZoneEvidenceKind, evidence)
}

func decodeHierarchyDeletionZoneEvidence(value []byte) (hierarchyDeletionZoneEvidence, error) {
	evidence, err := recordcodec.Decode[hierarchyDeletionZoneEvidence](value, hierarchyDeletionZoneEvidenceKind)
	if err != nil || validateHierarchyDeletionZoneEvidence(evidence) != nil {
		return hierarchyDeletionZoneEvidence{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return evidence, nil
}

func HierarchyDeletionZoneEvidenceAtRevision(
	ctx context.Context,
	store membershipStore,
	zoneID string,
	snapshotRevision int64,
	projectionRevision int64,
) (hierarchyDeletionZoneEvidence, []byte, error) {
	start := ""
	var matched *hierarchyDeletionZoneEvidence
	for {
		page, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: environmentqueries.EnvironmentDesiredHeadScanPrefix, StartExclusive: start,
			Limit: 200, Revision: snapshotRevision,
		})
		if err != nil {
			return hierarchyDeletionZoneEvidence{}, nil, err
		}
		if page == nil || page.ReadRevision != snapshotRevision {
			return hierarchyDeletionZoneEvidence{}, nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		for index := range page.Values {
			value := &page.Values[index]
			start = value.Key
			if !strings.HasSuffix(value.Key, "/current") {
				continue
			}
			environmentID := strings.TrimSuffix(
				strings.TrimPrefix(value.Key, environmentqueries.EnvironmentDesiredHeadScanPrefix), "/current",
			)
			if strings.Contains(environmentID, "/") || ids.Validate(ids.KindEnvironment, environmentID) != nil {
				return hierarchyDeletionZoneEvidence{}, nil, hierarchydeletion.CorruptHierarchyDeletion()
			}
			projection, found, projectionErr := blueprints.ReadCurrentProjection(
				ctx, store, environmentID, snapshotRevision,
			)
			if projectionErr != nil {
				return hierarchyDeletionZoneEvidence{}, nil, projectionErr
			}
			if !found {
				continue
			}
			for _, desired := range projection.Record.DesiredZones {
				if desired.Desired.ID != zoneID {
					continue
				}
				if matched != nil || projection.Revision != projectionRevision {
					return hierarchyDeletionZoneEvidence{}, nil, hierarchydeletion.CorruptHierarchyDeletion()
				}
				evidence, evidenceErr := newHierarchyDeletionZoneEvidence(projection, desired)
				if evidenceErr != nil {
					return hierarchyDeletionZoneEvidence{}, nil, evidenceErr
				}
				matched = &evidence
			}
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return hierarchyDeletionZoneEvidence{}, nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
	}
	if matched == nil {
		return hierarchyDeletionZoneEvidence{}, nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	value, err := encodeHierarchyDeletionZoneEvidence(*matched)
	if err != nil {
		return hierarchyDeletionZoneEvidence{}, nil, err
	}
	return *matched, value, nil
}
