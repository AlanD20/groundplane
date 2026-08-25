package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/AlanD20/groundplane/internal/architecturecheck"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("architecture-check", flag.ContinueOnError)
	flags.SetOutput(stderr)
	root := flags.String("root", ".", "repository root")
	baselinePath := flags.String("baseline", "architecture-baseline.json", "architecture baseline")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	path := *baselinePath
	if !filepath.IsAbs(path) {
		path = filepath.Join(*root, path)
	}
	ctx := context.Background()
	baseline, err := architecturecheck.ReadBaseline(ctx, path)
	if err != nil {
		fmt.Fprintf(stderr, "architecture-check: %v\n", err)
		return 2
	}
	findings, err := architecturecheck.Check(ctx, *root, baseline)
	if err != nil {
		fmt.Fprintf(stderr, "architecture-check: %v\n", err)
		return 2
	}
	if err := architecturecheck.WriteFindings(stdout, findings); err != nil {
		fmt.Fprintf(stderr, "architecture-check: write findings: %v\n", err)
		return 2
	}
	if len(findings) != 0 {
		return 1
	}
	return 0
}
