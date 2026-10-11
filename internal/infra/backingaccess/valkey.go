package backingaccess

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"time"

	access "github.com/AlanD20/groundplane/internal/common/backingaccess"
)

func (check *observation) valkey(ctx context.Context, request access.Request) {
	connection, err := check.dial(ctx, "tcp", net.JoinHostPort(request.Host, strconv.Itoa(int(request.Port))))
	if err != nil {
		return
	}
	defer func() { _ = connection.Close() }() // Socket close cannot preserve usable state.
	deadline, _ := ctx.Deadline()             // Check always supplies a bounded context.
	if err := connection.SetDeadline(deadline); err != nil {
		check.set(access.Authenticate, time.Now(), err, access.ProtocolFailure)
		return
	}
	cancellationDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = connection.Close() // Cancellation closes the owned socket.
		close(cancellationDone)
	})
	defer func() {
		if !stop() {
			<-cancellationDone
		}
	}()
	reader := bufio.NewReaderSize(connection, 4096)
	started := time.Now()
	if request.Authentication != access.None {
		arguments := [][]byte{[]byte("AUTH")}
		if request.Authentication == access.UsernamePassword {
			arguments = append(arguments, []byte(request.Username))
		}
		arguments = append(arguments, request.Password)
		reply, rejected, err := valkeyCommand(connection, reader, arguments)
		failure := access.ProtocolFailure
		if rejected {
			failure = access.AuthenticationFailed
		}
		if err == nil && (rejected || reply != "OK") {
			err = errors.New("authentication was not accepted")
		}
		check.set(access.Authenticate, started, withCancellation(ctx, err), failure)
		if err != nil {
			return
		}
	}
	started = time.Now()
	reply, rejected, err := valkeyCommand(connection, reader, [][]byte{[]byte("PING")})
	failure := access.AccessDenied
	if rejected && (reply == "NOAUTH" || reply == "WRONGPASS") {
		check.set(
			access.Authenticate,
			started,
			errors.New("configured authentication was not accepted"),
			access.AuthenticationFailed,
		)
		return
	}
	if err == nil && (rejected || reply != "PONG") {
		err = errors.New("fixed access command was not accepted")
	}
	check.set(access.DatabaseAccess, started, withCancellation(ctx, err), failure)
}

// valkeyCommand implements only RESP arrays of bulk-string requests and
// bounded simple-string/error replies. No caller supplies arbitrary commands.
func valkeyCommand(writer io.Writer, reader *bufio.Reader, arguments [][]byte) (string, bool, error) {
	var payload bytes.Buffer
	payload.WriteString("*" + strconv.Itoa(len(arguments)) + "\r\n")
	for _, argument := range arguments {
		payload.WriteString("$" + strconv.Itoa(len(argument)) + "\r\n")
		payload.Write(argument)
		payload.WriteString("\r\n")
	}
	owned := payload.Bytes()
	defer clear(owned)
	if _, err := io.Copy(writer, &payload); err != nil {
		return "", false, err
	}
	line, err := reader.ReadSlice('\n')
	defer clear(line)
	if err != nil {
		return "", false, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' || line[0] != '+' && line[0] != '-' {
		return "", false, errors.New("unexpected RESP reply")
	}
	if line[0] == '-' {
		// Discard provider text; retain only its fixed classification token.
		body := line[1 : len(line)-2]
		code, _, _ := bytes.Cut(body, []byte(" "))
		switch {
		case bytes.Equal(code, []byte("NOAUTH")):
			return "NOAUTH", true, nil
		case bytes.Equal(code, []byte("WRONGPASS")):
			return "WRONGPASS", true, nil
		case bytes.Equal(code, []byte("NOPERM")):
			return "NOPERM", true, nil
		default:
			return "REJECTED", true, nil
		}
	}
	return string(line[1 : len(line)-2]), false, nil
}

func withCancellation(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
