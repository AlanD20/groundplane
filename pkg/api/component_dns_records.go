package api

import (
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/dnsrecords"
)

func decodeDNSRecords(raw json.RawMessage) ([]dnsrecords.DNSRecord, error) {
	if !present(raw) {
		return nil, nil
	}
	var values []json.RawMessage
	if err := decodeRequiredField(raw, "records", &values); err != nil {
		return nil, err
	}
	records := make([]dnsrecords.DNSRecord, len(values))
	for i, value := range values {
		if err := decodeComponentConfigWire(value, &records[i]); err != nil {
			return nil, err
		}
	}
	return records, dnsrecords.ValidateDNSRecords(records)
}
