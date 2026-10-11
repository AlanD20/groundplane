// Package backingaccess defines the closed, read-only consumer access check.
// Requests are private machine input. Results never contain credential values
// or provider error text.
package backingaccess

import (
	"bytes"
	"net"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const Timeout = 20 * time.Second

type Family string

const (
	PostgreSQL Family = "postgres"
	MySQL      Family = "mysql"
	Valkey     Family = "valkey"
)

type Authentication string

const (
	UsernamePassword Authentication = "username_password"
	Password         Authentication = "password"
	None             Authentication = "none"
)

// Request selects one already-resolved credential set. The executor must run
// in the selected consumer's network namespace, not the Backing container's.
// Password is delivered over private stdin, never argv, logs or container Env.
type Request struct {
	AttachID           string         `json:"attach_id"`
	CredentialAttachID string         `json:"credential_attach_id"`
	EnvironmentID      string         `json:"environment_id"`
	ServiceID          string         `json:"service_id"`
	BackingServiceID   string         `json:"backing_service_id"`
	Family             Family         `json:"family"`
	Authentication     Authentication `json:"authentication"`
	Host               string         `json:"host"`
	Port               uint16         `json:"port"`
	Database           string         `json:"database,omitempty"`
	Username           string         `json:"username,omitempty"`
	Password           []byte         `json:"password,omitempty"`
}

func (request Request) Validate() error {
	if ids.Validate(ids.KindAttach, request.AttachID) != nil ||
		ids.Validate(ids.KindAttach, request.CredentialAttachID) != nil ||
		ids.Validate(ids.KindEnvironment, request.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, request.ServiceID) != nil ||
		ids.Validate(ids.KindService, request.BackingServiceID) != nil ||
		request.Port == 0 || !validHost(request.Host) ||
		!validText(request.Database, 255) || !validText(request.Username, 255) ||
		len(request.Password) > 64<<10 || !utf8.Valid(request.Password) ||
		bytes.IndexByte(request.Password, 0) >= 0 {
		return errs.New(errs.KindValidationFailed, "Backing access check input is invalid")
	}
	switch request.Family {
	case PostgreSQL, MySQL:
		if request.Authentication != UsernamePassword || request.Database == "" || request.Username == "" {
			return errs.New(
				errs.KindValidationFailed,
				"database access check requires its consumer database and credentials",
			)
		}
	case Valkey:
		if request.Database != "" {
			return errs.New(errs.KindValidationFailed, "Valkey access check does not select a database")
		}
		switch request.Authentication {
		case UsernamePassword:
			if request.Username == "" {
				return errs.New(errs.KindValidationFailed, "Valkey username and password mode requires a username")
			}
		case Password:
			if request.Username != "" {
				return errs.New(errs.KindValidationFailed, "Valkey password mode must not provide a username")
			}
		case None:
			if request.Username != "" || len(request.Password) != 0 {
				return errs.New(errs.KindValidationFailed, "Valkey no-auth mode must not provide credentials")
			}
		default:
			return errs.New(errs.KindValidationFailed, "Valkey access check requires an explicit authentication mode")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Backing access checks support PostgreSQL, MySQL and Valkey")
	}
	return nil
}

func (request *Request) Clear() {
	clear(request.Password)
	request.Password = nil
}

func validText(value string, maximum int) bool {
	return len(value) <= maximum && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func validHost(host string) bool {
	if net.ParseIP(host) != nil {
		return true
	}
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if character != '-' && !(character >= 'a' && character <= 'z') &&
				!(character >= 'A' && character <= 'Z') && !(character >= '0' && character <= '9') {
				return false
			}
		}
	}
	return true
}

type StageName string

const (
	Network        StageName = "network"
	Authenticate   StageName = "authentication"
	DatabaseAccess StageName = "database_access"
)

type Outcome string

const (
	Passed  Outcome = "passed"
	Failed  Outcome = "failed"
	Skipped Outcome = "skipped"
)

type Failure string

const (
	NoFailure            Failure = ""
	ConnectionFailure    Failure = "connection"
	AuthenticationFailed Failure = "authentication"
	AccessDenied         Failure = "database_access"
	TimeoutFailure       Failure = "timeout"
	Canceled             Failure = "canceled"
	ProtocolFailure      Failure = "protocol"
)

type Stage struct {
	Name    StageName     `json:"name"`
	Outcome Outcome       `json:"outcome"`
	Failure Failure       `json:"failure,omitempty"`
	Elapsed time.Duration `json:"elapsed"`
}

type Result struct {
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Stages     []Stage   `json:"stages"`
}

func (result Result) Validate() error {
	if result.StartedAt.IsZero() || result.FinishedAt.Before(result.StartedAt) || len(result.Stages) != 3 {
		return errs.New(errs.KindValidationFailed, "Backing access check result is invalid")
	}
	for index, expected := range []StageName{Network, Authenticate, DatabaseAccess} {
		stage := result.Stages[index]
		if stage.Name != expected || stage.Elapsed < 0 || stage.Elapsed > result.FinishedAt.Sub(result.StartedAt) {
			return errs.New(errs.KindValidationFailed, "Backing access check stage is invalid")
		}
		switch stage.Outcome {
		case Passed, Skipped:
			if stage.Failure != NoFailure {
				return errs.New(errs.KindValidationFailed, "Backing access check successful stage has a failure")
			}
		case Failed:
			switch stage.Failure {
			case ConnectionFailure, AuthenticationFailed, AccessDenied, TimeoutFailure, Canceled, ProtocolFailure:
			default:
				return errs.New(errs.KindValidationFailed, "Backing access check failure is invalid")
			}
		default:
			return errs.New(errs.KindValidationFailed, "Backing access check outcome is invalid")
		}
	}
	return nil
}

// ValidateFor rejects success that skips a required earlier stage. A database
// startup rejection may leave authentication unobserved; it must not imply a
// successful authentication or successful access.
func (result Result) ValidateFor(request Request) error {
	if err := result.Validate(); err != nil {
		return err
	}
	network, authentication, database := result.Stages[0], result.Stages[1], result.Stages[2]
	if network.Outcome != Passed && (authentication.Outcome == Passed || database.Outcome == Passed) ||
		authentication.Outcome == Failed && database.Outcome != Skipped ||
		network.Outcome == Failed && (authentication.Outcome != Skipped || database.Outcome != Skipped) ||
		database.Outcome == Passed && authentication.Outcome != Passed && request.Authentication != None ||
		request.Authentication == None && authentication.Outcome == Passed {
		return errs.New(errs.KindValidationFailed, "Backing access result does not match its configured check stages")
	}
	return nil
}
