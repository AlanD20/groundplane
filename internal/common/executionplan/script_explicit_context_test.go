package executionplan

import (
	"fmt"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/scriptpolicy"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: immutable machine context is canonical and bounded before hashing;
// semantically ambiguous lists cannot masquerade as distinct execution authority.
func TestExplicitScriptContextRejectsNoncanonicalGrants(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*agentpb.ScriptExplicitExecutionContext)
	}{
		{"nil volume", func(context *agentpb.ScriptExplicitExecutionContext) { context.Volumes[0] = nil }},
		{"wrong volume kind", func(context *agentpb.ScriptExplicitExecutionContext) {
			context.Volumes[0].VolumeId = explicitEntryID
		}},
		{"duplicate volume", func(context *agentpb.ScriptExplicitExecutionContext) {
			context.Volumes = append(context.Volumes, context.Volumes[0])
		}},
		{"unsorted volumes", func(context *agentpb.ScriptExplicitExecutionContext) {
			context.Volumes = append(context.Volumes, &agentpb.ScriptExplicitVolumeGrant{
				VolumeId: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAT", Target: "/other",
			})
		}},
		{"overlapping volumes", func(context *agentpb.ScriptExplicitExecutionContext) {
			context.Volumes = append(context.Volumes, &agentpb.ScriptExplicitVolumeGrant{
				VolumeId: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAW", Target: "/storage/child",
			})
		}},
		{"unsafe target", func(context *agentpb.ScriptExplicitExecutionContext) { context.Volumes[0].Target = "/proc" }},
		{"unclean target", func(context *agentpb.ScriptExplicitExecutionContext) {
			context.Volumes[0].Target = "/application/../storage"
		}},
		{"duplicate entries", func(context *agentpb.ScriptExplicitExecutionContext) {
			context.EntryIds = append(context.EntryIds, context.EntryIds[0])
		}},
		{"unsorted entries", func(context *agentpb.ScriptExplicitExecutionContext) {
			context.EntryIds = append(context.EntryIds, "ev_01ARZ3NDEKTSV4RRFFQ69G5FAT")
		}},
		{"wrong entry kind", func(context *agentpb.ScriptExplicitExecutionContext) { context.EntryIds[0] = explicitVolumeID }},
	} {
		t.Run(test.name, func(t *testing.T) {
			context := explicitScriptGrantedPlanForTest(t).ScriptRunnerSnapshots[0].ExplicitExecution.Context
			if _, err := ScriptExecutionContextDigest(context); err != nil {
				t.Fatalf("valid context rejected: %v", err)
			}
			test.edit(context)
			if _, err := ScriptExecutionContextDigest(context); err == nil {
				t.Fatal("noncanonical context accepted")
			}
		})
	}
}

// Rationale: grant limits apply to distinct valid resources, not only duplicates;
// the exact documented bound works and its first excess is rejected.
func TestExplicitScriptContextGrantLimits(t *testing.T) {
	for _, resource := range []string{"volumes", "entries"} {
		t.Run(resource, func(t *testing.T) {
			context := &agentpb.ScriptExplicitExecutionContext{
				ImageReference: "example/setup@sha256:" + strings.Repeat("b", 64), User: "0:0",
			}
			limit := scriptpolicy.MaximumEntries
			if resource == "volumes" {
				limit = scriptpolicy.MaximumVolumes
			}
			for index := 0; index <= limit; index++ {
				if resource == "volumes" {
					context.Volumes = append(context.Volumes, &agentpb.ScriptExplicitVolumeGrant{
						VolumeId: fmt.Sprintf("vol_%026d", index+1), Target: fmt.Sprintf("/storage%d", index),
					})
				} else {
					context.EntryIds = append(context.EntryIds, fmt.Sprintf("ev_%026d", index+1))
				}
				_, err := ScriptExecutionContextDigest(context)
				if index < limit && err != nil {
					t.Fatalf("%d valid grants rejected: %v", index+1, err)
				}
				if index == limit && err == nil {
					t.Fatal("first excess grant accepted")
				}
			}
		})
	}
}
