// Package backingaccess executes the fixed consumer queries inside a network
// context supplied by the Agent. It never provisions or modifies application data.
package backingaccess

import (
	"context"
	"errors"
	"net"
	"time"

	access "github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type observation struct {
	result access.Result
	dialed bool
}

func Check(ctx context.Context, request access.Request) (access.Result, error) {
	if ctx == nil {
		return access.Result{}, errs.New(errs.KindValidationFailed, "Backing access check context is required")
	}
	if err := request.Validate(); err != nil {
		return access.Result{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, access.Timeout)
	defer cancel()
	check := &observation{result: access.Result{StartedAt: time.Now().UTC(), Stages: []access.Stage{
		{Name: access.Network, Outcome: access.Skipped},
		{Name: access.Authenticate, Outcome: access.Skipped},
		{Name: access.DatabaseAccess, Outcome: access.Skipped},
	}}}
	switch request.Family {
	case access.PostgreSQL:
		check.postgres(ctx, request)
	case access.MySQL:
		check.mysql(ctx, request)
	case access.Valkey:
		check.valkey(ctx, request)
	}
	check.result.FinishedAt = time.Now().UTC()
	return check.result, nil
}

func (check *observation) dial(ctx context.Context, network, address string) (net.Conn, error) {
	if check.dialed {
		return nil, errors.New("access check does not retry connections")
	}
	check.dialed = true
	started := time.Now()
	connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
	check.set(access.Network, started, err, access.ConnectionFailure)
	if err == nil {
		connection = &boundedConnection{Conn: connection, remaining: 1 << 20}
	}
	return connection, err
}

// A misbehaving endpoint cannot stream unbounded SQL rows or protocol replies
// into memory during the bounded check. All clients use this same connection.
type boundedConnection struct {
	net.Conn
	remaining int
}

func (connection *boundedConnection) Read(value []byte) (int, error) {
	if connection.remaining == 0 {
		return 0, errors.New("access check response exceeds its bound")
	}
	if len(value) > connection.remaining {
		value = value[:connection.remaining]
	}
	read, err := connection.Conn.Read(value)
	connection.remaining -= read
	return read, err
}

func (check *observation) set(name access.StageName, started time.Time, err error, failure access.Failure) {
	for index := range check.result.Stages {
		if check.result.Stages[index].Name != name {
			continue
		}
		stage := &check.result.Stages[index]
		stage.Elapsed = time.Since(started)
		stage.Outcome, stage.Failure = access.Passed, access.NoFailure
		if err != nil {
			stage.Outcome, stage.Failure = access.Failed, failureFor(err, failure)
		}
		return
	}
}

func failureFor(err error, fallback access.Failure) access.Failure {
	if errors.Is(err, context.DeadlineExceeded) {
		return access.TimeoutFailure
	}
	if errors.Is(err, context.Canceled) {
		return access.Canceled
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return access.TimeoutFailure
	}
	return fallback
}
