package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/swarmcheck"
	"github.com/AlanD20/groundplane/internal/swarmgit"
)

type inspectionOutput struct {
	Phase     swarmcheck.Phase    `json:"phase"`
	Head      swarmcheck.CommitID `json:"head"`
	IndexTree swarmcheck.TreeID   `json:"index_tree"`
	Clean     bool                `json:"clean"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) != 0 && args[0] == "prove-gate" {
		return runProveGate(ctx, args[1:], stdout, stderr)
	}
	return runValidate(ctx, args, stdout, stderr)
}

func runValidate(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("swarm-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("root", ".", "repository root")
	manifestPath := flags.String("manifest", "", "swarm manifest path")
	lane := flags.String("lane", "", "writer lane for the writing phase")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "swarm-check: positional arguments are not accepted")
		return 2
	}
	if *manifestPath == "" {
		fmt.Fprintln(stderr, "swarm-check: -manifest is required")
		return 2
	}
	absoluteRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintf(stderr, "swarm-check: resolve root: %v\n", err)
		return 2
	}
	path := *manifestPath
	if !filepath.IsAbs(path) {
		path = filepath.Join(absoluteRoot, path)
	}
	manifest, err := swarmcheck.ReadManifest(ctx, path)
	if err != nil {
		fmt.Fprintf(stderr, "swarm-check: %v\n", err)
		return 1
	}
	expectedManifest := filepath.Join(
		absoluteRoot,
		filepath.FromSlash(string(swarmcheck.ManifestPath(manifest.Wave))),
	)
	if filepath.Clean(path) != filepath.Clean(expectedManifest) {
		fmt.Fprintf(
			stderr,
			"swarm-check: manifest must be at %s\n",
			swarmcheck.ManifestPath(manifest.Wave),
		)
		return 1
	}
	if err := validateLaneSelection(manifest.Phase, *lane); err != nil {
		fmt.Fprintf(stderr, "swarm-check: %v\n", err)
		return 2
	}
	inspector := swarmgit.NewInspector(absoluteRoot, runner.New(nil))
	switch manifest.Phase {
	case swarmcheck.PhaseDispatch:
		err = inspector.ValidateDispatch(ctx, manifest)
	case swarmcheck.PhaseWriting:
		if err = inspector.ValidateWritingEvidence(ctx, manifest); err != nil {
			break
		}
		writers := manifest.Writers
		if *lane != "" {
			writers = nil
			for _, candidate := range manifest.Writers {
				if candidate.Lane == swarmcheck.Lane(*lane) {
					writers = append(writers, candidate)
					break
				}
			}
			if len(writers) == 0 {
				fmt.Fprintf(stderr, "swarm-check: unknown writer lane %q\n", *lane)
				return 2
			}
		}
		for _, writer := range writers {
			assignment, exists := swarmcheck.ResolveWritingAssignment(manifest, writer.Lane)
			if !exists {
				err = fmt.Errorf("resolve writing assignment for lane %s", writer.Lane)
				break
			}
			writerInspector := swarmgit.NewInspector(
				filepath.Join(absoluteRoot, filepath.FromSlash(string(assignment.Worktree))),
				runner.New(nil),
			)
			if err = writerInspector.ValidateWriting(ctx, manifest, writer.Lane); err != nil {
				break
			}
		}
	default:
		err = inspector.ValidateIntegration(ctx, manifest)
	}
	if err != nil {
		fmt.Fprintf(stderr, "swarm-check: %v\n", err)
		return 1
	}
	state, err := inspector.Inspect(ctx, manifest)
	if err != nil {
		fmt.Fprintf(stderr, "swarm-check: %v\n", err)
		return 1
	}
	if err := json.NewEncoder(stdout).
		Encode(inspectionOutput{Phase: manifest.Phase, Head: state.Head, IndexTree: state.IndexTree, Clean: state.Clean}); err != nil {
		fmt.Fprintf(stderr, "swarm-check: write result: %v\n", err)
		return 2
	}
	return 0
}

func validateLaneSelection(phase swarmcheck.Phase, lane string) error {
	if lane != "" && phase != swarmcheck.PhaseWriting {
		return fmt.Errorf("-lane is accepted only in the writing phase")
	}
	return nil
}

func runProveGate(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("swarm-check prove-gate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("root", ".", "repository root")
	manifestPath := flags.String("manifest", "", "swarm manifest path")
	gateValue := flags.String("gate", "", "closed gate id")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "swarm-check prove-gate: positional arguments are not accepted")
		return 2
	}
	if *manifestPath == "" || *gateValue == "" {
		fmt.Fprintln(stderr, "swarm-check prove-gate: -manifest and -gate are required")
		return 2
	}
	absoluteRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintf(stderr, "swarm-check prove-gate: resolve root: %v\n", err)
		return 2
	}
	path := *manifestPath
	if !filepath.IsAbs(path) {
		path = filepath.Join(absoluteRoot, path)
	}
	manifest, err := swarmcheck.ReadManifest(ctx, path)
	if err != nil {
		fmt.Fprintf(stderr, "swarm-check prove-gate: %v\n", err)
		return 1
	}
	expectedManifest := filepath.Join(
		absoluteRoot,
		filepath.FromSlash(string(swarmcheck.ManifestPath(manifest.Wave))),
	)
	if filepath.Clean(path) != filepath.Clean(expectedManifest) {
		fmt.Fprintf(
			stderr,
			"swarm-check prove-gate: manifest must be at %s\n",
			swarmcheck.ManifestPath(manifest.Wave),
		)
		return 1
	}
	receipt, proveErr := swarmgit.NewInspector(absoluteRoot, runner.New(nil)).ProveGate(
		ctx,
		manifest,
		swarmcheck.GateID(*gateValue),
	)
	if receipt.Artifact != "" {
		if err := json.NewEncoder(stdout).Encode(receipt); err != nil {
			fmt.Fprintf(stderr, "swarm-check prove-gate: write receipt: %v\n", err)
			return 2
		}
	}
	if proveErr != nil {
		fmt.Fprintf(stderr, "swarm-check prove-gate: %v\n", proveErr)
		return 1
	}
	return 0
}
