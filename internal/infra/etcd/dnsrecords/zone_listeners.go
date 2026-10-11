package dnsrecords

import (
	"context"
	"maps"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	model "github.com/AlanD20/groundplane/internal/common/zonedns"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

// ResolveZoneDNSCandidates selects current authored Zone decisions at one
// revision. The global DNS generation fences newly published opted-in heads;
// the selected head comparisons fence replacements of already selected owners.
func (reader *Reader) ResolveZoneDNSCandidates(
	ctx context.Context,
	revision int64,
) ([]model.Candidate, []keyvalue.Condition, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, nil, err
	}
	if revision <= 0 {
		return nil, nil, errs.New(errs.KindValidationFailed, "DNS Zone selection requires a captured read revision")
	}
	generation, err := reader.routeNameCondition(ctx, revision)
	if err != nil {
		return nil, nil, err
	}
	conditions := []keyvalue.Condition{generation}
	var candidates []model.Candidate
	start := ""
	for {
		page, err := reader.store.Range(ctx, keyvalue.RangeRequest{
			Prefix:         environmentqueries.EnvironmentDesiredHeadScanPrefix,
			StartExclusive: start, Limit: keyvalue.MaximumPageLimit, Revision: revision,
		})
		if err != nil {
			return nil, nil, err
		}
		if page == nil || page.ReadRevision != revision || page.More && len(page.Values) == 0 {
			return nil, nil, errs.New(errs.KindInternal, "DNS Zone head scan is incomplete")
		}
		for _, value := range page.Values {
			start = value.Key
			if !strings.HasSuffix(value.Key, "/current") {
				continue
			}
			environmentID := strings.TrimSuffix(strings.TrimPrefix(value.Key,
				environmentqueries.EnvironmentDesiredHeadScanPrefix), "/current")
			if ids.Validate(ids.KindEnvironment, environmentID) != nil {
				return nil, nil, errs.New(errs.KindInternal, "DNS Zone head owner is invalid")
			}
			desired, found, err := blueprints.ReadCurrentDesiredInput(ctx, reader.store, environmentID, revision)
			if err != nil {
				return nil, nil, err
			}
			if !found {
				return nil, nil, errs.New(errs.KindInternal, "DNS Zone head has no authored input")
			}
			decisions, err := zoneDNSDecisions(desired.Record.Input.NormalizedCompose)
			if err != nil {
				return nil, nil, err
			}
			if len(decisions) == 0 {
				continue
			}
			identities, _, err := blueprints.ReadCurrentOwnedIdentities(ctx, reader.store, environmentID, revision)
			if err != nil {
				return nil, nil, err
			}
			remaining := maps.Clone(decisions)
			for _, identity := range identities.Record.Networks {
				if subnet, enabled := decisions[identity.Name]; enabled {
					withdrawing, condition, err := reader.zoneListenerWithdrawing(ctx, identity.ID, revision)
					if err != nil {
						return nil, nil, err
					}
					conditions = append(conditions, condition)
					if !withdrawing {
						candidates = append(candidates, model.Candidate{
							ZoneID: identity.ID, EnvironmentID: environmentID, Subnet: subnet,
						})
					}
					delete(remaining, identity.Name)
				}
			}
			if len(remaining) != 0 {
				return nil, nil, errs.New(errs.KindInternal, "DNS Zone has no owned identity")
			}
			if len(candidates) > 256 {
				return nil, nil, errs.New(errs.KindValidationFailed, "at most 256 internal DNS Zones are supported")
			}
			conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
		}
		if !page.More {
			break
		}
	}
	conditions, err = uniqueConditions(conditions)
	return candidates, conditions, err
}

// Native removal admission publishes this exact resource fence atomically with
// its Task and intent. Direct removal retains the old desired head until success,
// so the admitted fence, not head omission, initiates listener withdrawal.
func (reader *Reader) zoneListenerWithdrawing(
	ctx context.Context,
	zoneID string,
	revision int64,
) (bool, keyvalue.Condition, error) {
	key := deletions.TombstoneKey(string(deletions.DeletionTargetZone), zoneID)
	read, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return false, keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return false, keyvalue.Condition{}, errs.New(errs.KindInternal, "DNS Zone removal fence read is incomplete")
	}
	condition := keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])}
	if read.Values[0] == nil {
		return false, condition, nil
	}
	tombstone, err := deletions.DecodeDeletionTombstone(read.Values[0].Value)
	if err != nil {
		return false, keyvalue.Condition{}, err
	}
	if tombstone.TargetKind != deletions.DeletionTargetZone || tombstone.TargetID != zoneID {
		return false, keyvalue.Condition{}, errs.New(
			errs.KindStateConflict,
			"DNS Zone removal fence belongs to another resource",
		)
	}
	return true, condition, nil
}

// Only accepted normalized input is read here. Generated service settings and
// ownership metadata cannot become a new listener decision.
func zoneDNSDecisions(content []byte) (map[string]string, error) {
	if len(content) == 0 {
		return nil, nil
	}
	var document struct {
		Networks map[string]struct {
			Internal bool `yaml:"internal"`
			Allow    bool `yaml:"x-gp-allow-dns"`
			IPAM     struct {
				Config []struct {
					Subnet string `yaml:"subnet"`
				} `yaml:"config"`
			} `yaml:"ipam"`
		} `yaml:"networks"`
	}
	if err := yaml.Unmarshal(content, &document); err != nil {
		return nil, errs.New(errs.KindInternal, "DNS Zone authored topology is corrupt")
	}
	result := make(map[string]string)
	for name, network := range document.Networks {
		if !network.Allow {
			continue
		}
		if !network.Internal || len(network.IPAM.Config) != 1 {
			return nil, errs.New(errs.KindInternal, "DNS Zone authored topology is invalid")
		}
		subnet := network.IPAM.Config[0].Subnet
		if _, err := model.Gateway(subnet); err != nil {
			return nil, err
		}
		result[name] = subnet
	}
	return result, nil
}
