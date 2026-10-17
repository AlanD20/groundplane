package network

import (
	"slices"

	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func orderBackingZoneCascadeAttaches(
	attaches []etcdstore.Versioned[attachrecord.Record],
) ([]etcdstore.Versioned[attachrecord.Record], error) {
	byID := make(map[string]etcdstore.Versioned[attachrecord.Record], len(attaches))
	indegree := make(map[string]int, len(attaches))
	edges := make(map[string][]string, len(attaches))
	for _, attach := range attaches {
		if _, exists := byID[attach.Record.ID]; exists {
			return nil, errs.New(errs.KindInternal, "backing Zone cascade contains duplicate Attach identity")
		}
		byID[attach.Record.ID] = attach
		indegree[attach.Record.ID] = 0
	}
	for _, attach := range attaches {
		targets := slices.Clone(attach.Record.GrantAttachIDs)
		if !attach.Record.OwnsCredential() {
			if _, selected := byID[attach.Record.CredentialAttachID]; selected &&
				!slices.Contains(targets, attach.Record.CredentialAttachID) {
				targets = append(targets, attach.Record.CredentialAttachID)
			}
		}
		for _, grantID := range targets {
			if _, exists := byID[grantID]; !exists {
				return nil, errs.New(errs.KindInternal, "backing Zone cascade grant target is missing")
			}
			owner := byID[grantID].Record
			if owner.BackingServiceID != attach.Record.BackingServiceID || !owner.OwnsCredential() {
				return nil, errs.New(errs.KindStateConflict, "Backing Zone credential dependency has a different owner")
			}
			edges[attach.Record.ID] = append(edges[attach.Record.ID], grantID)
			indegree[grantID]++
		}
	}
	ready := make([]string, 0, len(attaches))
	for id, degree := range indegree {
		if degree == 0 {
			ready = append(ready, id)
		}
	}
	slices.Sort(ready)
	result := make([]etcdstore.Versioned[attachrecord.Record], 0, len(attaches))
	for len(ready) != 0 {
		id := ready[0]
		ready = ready[1:]
		result = append(result, byID[id])
		for _, target := range edges[id] {
			indegree[target]--
			if indegree[target] == 0 {
				ready = append(ready, target)
				slices.Sort(ready)
			}
		}
	}
	if len(result) != len(attaches) {
		return nil, errs.New(errs.KindInternal, "backing Zone cascade Attach grants contain a cycle")
	}
	return result, nil
}
