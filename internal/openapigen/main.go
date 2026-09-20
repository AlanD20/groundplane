// Command openapigen writes the Controller's code-first OpenAPI document.
package main

import (
	"errors"
	"flag"
	"fmt"
	handlers "github.com/AlanD20/groundplane/internal/controller/handlers"
	"os"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "openapigen:", err)
		os.Exit(1)
	}
}

func run() error {
	output := flag.String("output", "", "path to the generated OpenAPI JSON document")
	flag.Parse()
	if *output == "" {
		return errors.New("-output is required")
	}
	document, err := handlers.New(nil, nil, handlers.Options{}).OpenAPIDocument()
	if err != nil {
		return err
	}
	temporary := *output + ".tmp"
	if err := os.WriteFile(temporary, document, 0o644); err != nil {
		return fmt.Errorf("write temporary document: %w", err)
	}
	if err := os.Rename(temporary, *output); err != nil {
		_ = os.Remove(temporary)
		return fmt.Errorf("publish document: %w", err)
	}
	return nil
}
