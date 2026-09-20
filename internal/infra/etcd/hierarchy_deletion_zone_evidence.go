package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
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
	projection Versioned[EnvironmentComposeProjection],
	desired EnvironmentZoneProjection,
) (hierarchyDeletionZoneEvidence, error) {
	evidence := hierarchyDeletionZoneEvidence{
		EnvironmentID: desired.EnvironmentID, ZoneID: desired.Desired.ID,
		Desired: desired.Desired, ProjectionRevision: projection.Revision,
	}
	if projection.ReadRevision <= 0 || projection.Revision <= 0 ||
		projection.Record.EnvironmentID != desired.EnvironmentID ||
		validateHierarchyDeletionZoneEvidence(evidence) != nil {
		return hierarchyDeletionZoneEvidence{}, corruptHierarchyDeletion()
	}
	return evidence, nil
}

func validateHierarchyDeletionZoneEvidence(evidence hierarchyDeletionZoneEvidence) error {
	if evidence.ProjectionRevision <= 0 || evidence.ZoneID != evidence.Desired.ID ||
		validateZoneRecord(ZoneRecord{EnvironmentID: evidence.EnvironmentID, Desired: evidence.Desired}) != nil {
		return corruptHierarchyDeletion()
	}
	return nil
}

func encodeHierarchyDeletionZoneEvidence(evidence hierarchyDeletionZoneEvidence) ([]byte, error) {
	if err := validateHierarchyDeletionZoneEvidence(evidence); err != nil {
		return nil, err
	}
	return encodeEnvelope(hierarchyDeletionZoneEvidenceKind, evidence)
}

func decodeHierarchyDeletionZoneEvidence(value []byte) (hierarchyDeletionZoneEvidence, error) {
	evidence, err := decodeEnvelope[hierarchyDeletionZoneEvidence](value, hierarchyDeletionZoneEvidenceKind)
	if err != nil || validateHierarchyDeletionZoneEvidence(evidence) != nil {
		return hierarchyDeletionZoneEvidence{}, corruptHierarchyDeletion()
	}
	return evidence, nil
}

func hierarchyDeletionZoneEvidenceAtRevision(
	ctx context.Context,
	store hierarchyStore,
	zoneID string,
	snapshotRevision int64,
	projectionRevision int64,
) (hierarchyDeletionZoneEvidence, []byte, error) {
	start := ""
	var matched *hierarchyDeletionZoneEvidence
	for {
		page, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: environmentDesiredHeadScanPrefix, StartExclusive: start,
			Limit: 200, Revision: snapshotRevision,
		})
		if err != nil {
			return hierarchyDeletionZoneEvidence{}, nil, err
		}
		if page == nil || page.ReadRevision != snapshotRevision {
			return hierarchyDeletionZoneEvidence{}, nil, corruptHierarchyDeletion()
		}
		for index := range page.Values {
			value := &page.Values[index]
			start = value.Key
			if !strings.HasSuffix(value.Key, "/current") {
				continue
			}
			environmentID := strings.TrimSuffix(
				strings.TrimPrefix(value.Key, environmentDesiredHeadScanPrefix), "/current",
			)
			if strings.Contains(environmentID, "/") || ids.Validate(ids.KindEnvironment, environmentID) != nil {
				return hierarchyDeletionZoneEvidence{}, nil, corruptHierarchyDeletion()
			}
			projection, found, projectionErr := currentEnvironmentProjectionAtRevision(
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
					return hierarchyDeletionZoneEvidence{}, nil, corruptHierarchyDeletion()
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
			return hierarchyDeletionZoneEvidence{}, nil, corruptHierarchyDeletion()
		}
	}
	if matched == nil {
		return hierarchyDeletionZoneEvidence{}, nil, corruptHierarchyDeletion()
	}
	value, err := encodeHierarchyDeletionZoneEvidence(*matched)
	if err != nil {
		return hierarchyDeletionZoneEvidence{}, nil, err
	}
	return *matched, value, nil
}
