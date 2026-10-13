package backingaccess

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"

	access "github.com/AlanD20/groundplane/internal/common/backingaccess"
)

func (check *observation) postgres(ctx context.Context, request access.Request) {
	// ParseConfig supplies required private driver initialization. Explicit
	// bootstrap settings suppress service/pass files, TLS files and environment
	// selections; only the captured consumer input is used on the connection.
	// The helper clears PGSERVICE. An explicit service='' would select a blank
	// service and still make the driver try to read a service file.
	config, err := pgconn.ParseConfig("host=localhost port=5432 user=groundplane dbname=postgres password=unused " +
		"servicefile='' passfile='' sslmode=disable sslnegotiation=postgres " +
		"connect_timeout=20 target_session_attrs=any channel_binding=disable require_auth='' " +
		"min_protocol_version=3.0 max_protocol_version=3.0")
	started := time.Now()
	if err != nil {
		check.set(access.Authenticate, started, err, access.ProtocolFailure)
		return
	}
	config.Host, config.Port = request.Host, request.Port
	config.User, config.Password, config.Database = request.Username, string(request.Password), request.Database
	defer func() { config.Password = "" }()
	config.Fallbacks = nil
	config.RuntimeParams = map[string]string{"application_name": "groundplane-access-check"}
	config.MaxProtocolMessageBodyLen = 64 << 10
	config.DialFunc = check.dial
	connection, err := pgconn.ConnectConfig(ctx, config)
	if err != nil {
		if check.result.Stages[0].Outcome == access.Failed {
			return
		}
		var provider *pgconn.PgError
		if errors.As(err, &provider) && (provider.Code == "3D000" || provider.Code == "42501") {
			// Startup failed before a fully authenticated session was observed.
			// Do not turn an inferred authentication into a passing stage.
			check.set(access.DatabaseAccess, started, withCancellation(ctx, err), access.AccessDenied)
		} else {
			failure := access.ProtocolFailure
			if errors.As(err, &provider) && strings.HasPrefix(provider.Code, "28") {
				failure = access.AuthenticationFailed
			}
			check.set(access.Authenticate, started, withCancellation(ctx, err), failure)
		}
		return
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// The driver closes its socket even when protocol shutdown fails.
		_ = connection.Close(cleanup)
	}()
	check.set(access.Authenticate, started, nil, access.NoFailure)
	started = time.Now()
	results, err := connection.Exec(ctx, "SELECT 1").ReadAll()
	if err == nil && (len(results) != 1 || len(results[0].Rows) != 1 ||
		len(results[0].Rows[0]) != 1 || string(results[0].Rows[0][0]) != "1") {
		err = errors.New("unexpected fixed query result")
	}
	check.set(access.DatabaseAccess, started, withCancellation(ctx, err), access.AccessDenied)
}

func (check *observation) mysql(ctx context.Context, request access.Request) {
	config := mysql.NewConfig()
	config.User, config.Passwd = request.Username, string(request.Password)
	defer func() { config.Passwd = "" }()
	config.Net, config.Addr = "tcp", net.JoinHostPort(request.Host, strconv.Itoa(int(request.Port)))
	config.Timeout, config.ReadTimeout, config.WriteTimeout = access.Timeout, access.Timeout, access.Timeout
	config.MaxAllowedPacket = 64 << 10
	config.DialFunc = check.dial
	// Database selection is a separate stage after authentication, not an
	// implicit DBName selection that conflates missing grants with bad passwords.
	config.DBName = ""
	config.Logger = &mysql.NopLogger{}
	started := time.Now()
	connector, err := mysql.NewConnector(config)
	if err != nil {
		check.set(access.Authenticate, started, err, access.ProtocolFailure)
		return
	}
	database := sql.OpenDB(connector)
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(0)
	defer func() {
		// No pooled connections remain after the operation.
		_ = database.Close()
	}()
	connection, err := database.Conn(ctx)
	if err != nil {
		if check.result.Stages[0].Outcome != access.Failed {
			failure := access.ProtocolFailure
			var provider *mysql.MySQLError
			if errors.As(err, &provider) && provider.Number == 1045 {
				failure = access.AuthenticationFailed
			}
			check.set(access.Authenticate, started, withCancellation(ctx, err), failure)
		}
		return
	}
	defer func() {
		// This connection is owned by the immediately closing single-use pool.
		_ = connection.Close()
	}()
	check.set(access.Authenticate, started, nil, access.NoFailure)
	started = time.Now()
	_, err = connection.ExecContext(ctx, "USE `"+strings.ReplaceAll(request.Database, "`", "``")+"`")
	if err == nil {
		var value int
		err = connection.QueryRowContext(ctx, "SELECT 1").Scan(&value)
		if err == nil && value != 1 {
			err = errors.New("unexpected fixed query result")
		}
	}
	check.set(access.DatabaseAccess, started, withCancellation(ctx, err), access.AccessDenied)
}
