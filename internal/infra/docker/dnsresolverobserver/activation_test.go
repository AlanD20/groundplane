package dnsresolverobserver

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"testing"
	"time"
)

type delayedResolverRuntime struct {
	values []runtimeEvidence
	calls  int
}

func (runtime *delayedResolverRuntime) Inspect(ctx context.Context, request Request) (runtimeEvidence, error) {
	runtime.calls++
	value := runtime.values[0]
	if len(runtime.values) > 1 {
		runtime.values = runtime.values[1:]
	}
	return observerRuntimeStub{evidence: value}.Inspect(ctx, request)
}

func (*delayedResolverRuntime) Close() error { return nil }

// DNS-02/04: startup metrics and a replaced Corefile precede CoreDNS's reload. Waiting
// must accept only the exact candidate after both observations converge, and
// must recheck mounted bytes rather than bless an intervening replacement.
func TestAwaitConfigurationAcrossReload(t *testing.T) {
	artifact := []byte(". {\n  reload\n  prometheus 127.0.0.1:9153\n  forward . 1.1.1.1\n}\n")
	effective := testEffectiveConfigSHA512(t, "/etc/coredns/Corefile", artifact)
	old := sha512.Sum512([]byte("previous configuration"))
	request := Request{
		ArtifactSHA256: sha256.Sum256(artifact), ArtifactTarget: "/etc/coredns/Corefile",
		ReloadMetric: "coredns_reload_version_info",
	}
	t.Run("fresh startup without reload metric", func(t *testing.T) {
		executor := &Executor{
			runtime: observerRuntimeStub{
				evidence: runtimeEvidence{artifact: artifact, servingConfiguration: &effective},
			},
			metrics: &observerMetricsStub{values: [][]byte{forwardMetrics(nil, 0, 0)}},
		}
		result, err := executor.awaitConfiguration(context.Background(), request)
		if err != nil || result.effectiveDigest != effective {
			t.Fatalf("fresh startup: digest=%x, error=%v", result.effectiveDigest, err)
		}
	})
	t.Run("reload metric cannot prove a failed candidate is serving", func(t *testing.T) {
		executor := &Executor{
			runtime: observerRuntimeStub{evidence: runtimeEvidence{artifact: artifact, servingConfiguration: &old}},
			metrics: &observerMetricsStub{values: [][]byte{forwardMetrics(&effective, 0, 0)}},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if _, err := executor.awaitConfiguration(ctx, request); err == nil {
			t.Fatal("a matching reload metric overruled the last successful serving configuration")
		}
	})
	for _, changed := range []bool{false, true} {
		name := "delayed reload and metric"
		if changed {
			name = "mounted candidate replaced during wait"
		}
		t.Run(name, func(t *testing.T) {
			candidate := runtimeEvidence{artifact: artifact}
			if changed {
				candidate.artifact = []byte("unexpected replacement")
			}
			runtime := &delayedResolverRuntime{values: []runtimeEvidence{
				{artifact: artifact, servingConfiguration: &old}, candidate,
			}}
			executor := &Executor{
				runtime: runtime,
				metrics: &observerMetricsStub{values: [][]byte{
					forwardMetrics(nil, 0, 0), forwardMetrics(&old, 0, 0), forwardMetrics(&effective, 0, 0),
				}},
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := executor.awaitConfiguration(ctx, request)
			if changed {
				if err == nil || runtime.calls != 2 {
					t.Fatalf("changed artifact: calls=%d, error=%v", runtime.calls, err)
				}
				return
			}
			if err != nil || runtime.calls != 3 || result.effectiveDigest != effective {
				t.Fatalf("reload: calls=%d, digest=%x, error=%v", runtime.calls, result.effectiveDigest, err)
			}
		})
	}
}
