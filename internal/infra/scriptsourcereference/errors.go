package scriptsourcereference

import "fmt"

type ErrorKind string

const (
	ErrorValidation ErrorKind = "validation"
	ErrorConflict   ErrorKind = "conflict"
	ErrorCorruption ErrorKind = "corruption"
)

type Error struct {
	Kind ErrorKind
	Text string
}

func (err *Error) Error() string { return fmt.Sprintf("%s: %s", err.Kind, err.Text) }

func validation(text string) error { return &Error{Kind: ErrorValidation, Text: text} }
func conflict(text string) error   { return &Error{Kind: ErrorConflict, Text: text} }
func corruption(text string) error { return &Error{Kind: ErrorCorruption, Text: text} }
