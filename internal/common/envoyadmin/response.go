package envoyadmin

import (
	"bufio"
	"bytes"
	"io"
	"net/http"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// The upstream Ubuntu image has Bash/coreutils, not an HTTP client. Only this
// fixed bounded read is admitted; the destructive admin API stays on loopback.
const ReadCommand = "env -i PATH=/usr/local/bin:/usr/bin:/bin timeout 4 bash --noprofile --norc -ec '" +
	"exec 3<>/dev/tcp/127.0.0.1/9901; " +
	"printf \"GET /config_dump HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n\" >&3; cat <&3'"

// ReadResponse opens captured output; it performs no network or process I/O.
// Both the transport bytes and decoded HTTP body must fit the caller's bound.
func ReadResponse(value []byte, maximum int64) ([]byte, error) {
	if maximum <= 0 || len(value) == 0 || int64(len(value)) > maximum {
		return nil, unavailable()
	}
	response, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(value)), nil)
	if err != nil {
		return nil, unavailable()
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, unavailable()
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maximum+1))
	if err != nil || int64(len(body)) > maximum {
		return nil, unavailable()
	}
	return body, nil
}

func unavailable() error {
	return errs.New(
		errs.KindRequestFailed,
		"Envoy's private admin endpoint did not return a complete configuration response",
	)
}
