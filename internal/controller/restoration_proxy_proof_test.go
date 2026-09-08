package controller

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: exact healthy container identities do not prove the proxy's
// active route. A real rendered prior config must be independently observed.
func assertRestorationProxyProbe(
	t *testing.T,
	request *agentpb.ComposeHelperRequest,
	predecessor *agentpb.ComposeArtifact,
) {
	t.Helper()
	for _, test := range []struct {
		name                                                 string
		compensate, activeMatches, reloadWorks, wantComplete bool
	}{
		{name: "wrong active probe"},
		{name: "matching active probe", activeMatches: true, wantComplete: true},
		{name: "reload observed", compensate: true, reloadWorks: true, wantComplete: true},
		{name: "reload has no effect", compensate: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertRestorationProxyScenario(
				t,
				request,
				predecessor,
				test.compensate,
				test.activeMatches,
				test.reloadWorks,
				test.wantComplete,
			)
		})
	}
}

func assertRestorationProxyScenario(
	t *testing.T,
	request *agentpb.ComposeHelperRequest,
	predecessor *agentpb.ComposeArtifact,
	compensate, activeMatches, reloadWorks, wantComplete bool,
) {
	t.Helper()
	request = proto.CloneOf(request)
	request.StepId = request.Plan.CandidateReleaseProcedure.Members[0].ServingPredecessor.ProbeStepId
	if compensate {
		request.StepId = request.Plan.CandidateReleaseProcedure.Members[0].ServingPredecessor.CompensateStepId
	}
	services := make(map[string]*agentpb.ComposeService)
	for _, service := range predecessor.Services {
		if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			services["bbbbbbbbbbbbbbbb"] = service
		} else if labelPairMap(service.ExpectedLabels)[composeLabelReleaseID] != "" {
			services["aaaaaaaaaaaaaaaa"] = service
		}
	}
	proxy := services["bbbbbbbbbbbbbbbb"]
	reads, reloads := 0, 0
	fake := runner.NewFake()
	fake.RunFunc = func(_ context.Context, options runner.RunCmdOpts) (runner.Result, error) {
		if options.Args[0] == "exec" {
			if slices.Contains(options.Args, "reload") {
				if !compensate ||
					!slices.Equal(
						options.Args,
						[]string{"exec", "--interactive", "bbbbbbbbbbbbbbbb", "caddy", "reload", "--config", "-"},
					) ||
					!bytes.Equal(options.Stdin, proxy.ProxyConfigJson) {
					t.Fatalf("unauthorized reload: %v", options.Args)
				}
				reloads++
				activeMatches = reloadWorks
				return runner.Result{}, nil
			}
			if !slices.Equal(
				options.Args,
				[]string{
					"exec",
					"bbbbbbbbbbbbbbbb",
					"wget",
					"--quiet",
					"--output-document=-",
					"http://127.0.0.1:2019/config/",
				},
			) {
				t.Fatalf("unexpected proxy command: %v", options.Args)
			}
			reads++
			if activeMatches {
				return runner.Result{Stdout: slices.Clone(proxy.ProxyConfigJson)}, nil
			}
			return runner.Result{Stdout: []byte(`{"apps":{}}`)}, nil // Wrong active Caddy configuration.
		}
		if options.Args[0] != "container" {
			t.Fatalf("read-only restoration probe mutated runtime: %v", options.Args)
		}
		if options.Args[1] == "ls" {
			return runner.Result{Stdout: []byte("aaaaaaaaaaaaaaaa\nbbbbbbbbbbbbbbbb\n")}, nil
		}
		service := services[options.Args[len(options.Args)-1]]
		if service == nil {
			t.Fatalf("unknown container: %v", options.Args)
		}
		switch options.Args[3] {
		case "{{json .Config.Labels}}":
			labels := labelPairMap(service.ExpectedLabels)
			labels["com.docker.compose.project"], labels["com.docker.compose.service"] = predecessor.ProjectName, service.ComposeName
			encoded, err := json.Marshal(labels)
			if err != nil {
				t.Fatal(err)
			}
			return runner.Result{Stdout: encoded}, nil
		case "{{.Config.Image}}":
			return runner.Result{Stdout: []byte(service.ImageReference)}, nil
		case "{{.Config.Image}}\n{{.Image}}":
			return runner.Result{Stdout: []byte(service.ImageReference + "\n" + service.ImageReference)}, nil
		case "{{.Config.Image}}\n{{.Image}}\n{{json .ImageManifestDescriptor}}":
			return runner.Result{
				Stdout: []byte(
					service.ImageReference + "\nsha256:" + hex.EncodeToString(service.ImageConfigDigest) + "\nnull",
				),
			}, nil
		case "{{json .State}}":
			return runner.Result{Stdout: []byte(`{"Running":true,"Health":{"Status":"healthy"}}`)}, nil
		}
		t.Fatalf("unexpected observation: %v", options.Args)
		return runner.Result{}, nil
	}
	response, err := composehelper.Execute(context.Background(), fake, request)
	wantOutcome := agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED
	if !compensate && !wantComplete {
		wantOutcome = agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_RESTORATION_REQUIRED
	}
	if wantComplete {
		wantOutcome = agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED
	}
	if err != nil || response.GetOutcome() != wantOutcome {
		t.Fatalf("active proxy result = %v, %v, want %v", response, err, wantOutcome)
	}
	if reads == 0 || compensate && (reloads != 1 || reads != 2) || !compensate && reloads != 0 {
		t.Fatalf("active config reads=%d reloads=%d", reads, reloads)
	}
	if wantComplete && !bytes.Equal(response.GetProxyEvidence().GetConfigSha256(), proxy.ProxyConfigSha256) {
		t.Fatal("completed proxy proof differs from observed sealed configuration")
	}
}
