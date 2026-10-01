package dnsresolverobserver

import (
	"bytes"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/moby/moby/api/pkg/stdcopy"
)

// DNS-02/04: real CoreDNS starts without a reload metric, and query bursts must
// not hide the last successful instance fingerprint. Docker framing and later
// successful reloads must preserve that authority; malformed evidence cannot.
func TestServingConfigurationFromProcessLog(t *testing.T) {
	old := sha512.Sum512([]byte("previous configuration"))
	current := sha512.Sum512([]byte("current configuration"))
	initial := fmt.Sprintf("[INFO] plugin/reload: Running configuration SHA512 = %x\n", old)
	reloaded := fmt.Sprintf("[INFO] plugin/reload: Running configuration SHA512 = %x\n", current)
	burst := strings.Repeat("[INFO] client query\n", 10000)
	for _, tty := range []bool{false, true} {
		for _, test := range []struct {
			name    string
			log     string
			want    *[sha512.Size]byte
			invalid bool
		}{
			{name: "startup beyond query tail", log: initial + burst, want: &old},
			{name: "last successful reload", log: initial + burst + reloaded + burst, want: &current},
			{name: "no successful startup", log: burst},
			{name: "invalid fingerprint", log: initial + "[INFO] plugin/reload: Running configuration SHA512 = invalid\n", invalid: true},
		} {
			t.Run(fmt.Sprintf("%s/tty=%t", test.name, tty), func(t *testing.T) {
				var stream bytes.Buffer
				if !tty {
					header := [8]byte{byte(stdcopy.Stderr)}
					binary.BigEndian.PutUint32(header[4:], uint32(len(test.log)))
					stream.Write(header[:])
				}
				if _, err := stream.WriteString(test.log); err != nil {
					t.Fatal(err)
				}
				got, err := readServingConfiguration(io.NopCloser(&stream), tty)
				if (err != nil) != test.invalid || (got == nil) != (test.want == nil) ||
					(got != nil && test.want != nil && *got != *test.want) {
					t.Fatalf("serving configuration=%v, error=%v, expected=%v", got, err, test.want)
				}
			})
		}
	}
}
