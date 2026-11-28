// Package ingressexecution defines the closed host publication exchanged for
// one authenticated public ingress assignment.
package ingressexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumConfigurationBytes = 256 << 10

type Receipt struct {
	RuntimeID string `json:"runtime_id"`
	Revision  int64  `json:"revision"`
	Digest    string `json:"digest"`
}

type Plan struct {
	HostID        string       `json:"host_id"`
	TaskID        string       `json:"task_id"`
	Revision      int64        `json:"revision"`
	Digest        string       `json:"digest"`
	Addresses     []netip.Addr `json:"addresses"`
	Configuration []byte       `json:"configuration"`
	Predecessor   *Receipt     `json:"predecessor,omitempty"`
}

func (plan Plan) Validate() error {
	if ids.Validate(ids.KindHost, plan.HostID) != nil || ids.Validate(ids.KindTask, plan.TaskID) != nil ||
		plan.Revision <= 0 || len(plan.Configuration) == 0 || len(plan.Configuration) > MaximumConfigurationBytes ||
		len(plan.Addresses) == 0 || len(plan.Addresses) > 16 ||
		plan.Predecessor != nil && (!plan.Predecessor.Valid() || plan.Predecessor.Revision >= plan.Revision) {
		return errs.New(errs.KindValidationFailed, "public ingress assignment is invalid")
	}
	sum := sha256.Sum256(plan.Configuration)
	if plan.Digest != "sha256:"+hex.EncodeToString(sum[:]) {
		return errs.New(errs.KindStateConflict, "public ingress configuration differs from its captured digest")
	}
	for index, address := range plan.Addresses {
		if !address.IsGlobalUnicast() || address.Is4In6() || address.Zone() != "" ||
			index > 0 && plan.Addresses[index-1].Compare(address) >= 0 {
			return errs.New(errs.KindValidationFailed, "public ingress assignment has an invalid listener address")
		}
	}
	return nil
}

// SHA256 seals the whole host plan, including the predecessor receipt and
// listener addresses, before Agent assignment.
func (plan Plan) SHA256() (string, error) {
	if err := plan.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

type Result string

const (
	Applied   Result = "applied"
	Restored  Result = "restored"
	Uncertain Result = "uncertain"
)

// Report carries runtime evidence or records uncertainty. Only independent
// observation of a known runtime is sufficient to settle ownership.
type Report struct {
	Result  Result   `json:"result"`
	Receipt *Receipt `json:"receipt,omitempty"`
}

func (report Report) Validate() error {
	switch report.Result {
	case Applied, Restored:
		if report.Receipt != nil && report.Receipt.Valid() {
			return nil
		}
	case Uncertain:
		if report.Receipt == nil {
			return nil
		}
	}
	return errs.New(errs.KindValidationFailed, "public ingress work report is invalid")
}

func (receipt Receipt) Valid() bool {
	if receipt.Revision <= 0 || len(receipt.RuntimeID) != 64 || len(receipt.Digest) != 71 ||
		receipt.Digest[:7] != "sha256:" {
		return false
	}
	return validHex(receipt.RuntimeID) && validHex(receipt.Digest[7:])
}

func validHex(value string) bool {
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}
