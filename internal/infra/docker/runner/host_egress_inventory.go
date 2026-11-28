package runner

import (
	"encoding/json"
	"net/netip"
	"strconv"
	"strings"

	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
)

type nftInventory struct {
	Entries []nftEntry `json:"nftables"`
}

type nftEntry struct {
	Table    *nftTableState  `json:"table"`
	Chain    *nftChainState  `json:"chain"`
	Rule     *nftRuleState   `json:"rule"`
	Metainfo json.RawMessage `json:"metainfo"`
}

type nftTableState struct {
	Family  string   `json:"family"`
	Name    string   `json:"name"`
	Comment string   `json:"comment"`
	Flags   []string `json:"flags"`
}

type nftChainState struct {
	Family string `json:"family"`
	Table  string `json:"table"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	Hook   string `json:"hook"`
	Prio   int    `json:"prio"`
	Policy string `json:"policy"`
}

type nftRuleState struct {
	Family string         `json:"family"`
	Table  string         `json:"table"`
	Chain  string         `json:"chain"`
	Expr   []nftStatement `json:"expr"`
}

type nftStatement struct {
	Match  *nftMatch       `json:"match"`
	Jump   *nftJump        `json:"jump"`
	Accept json.RawMessage `json:"accept"`
	Drop   json.RawMessage `json:"drop"`
}

type nftJump struct {
	Target string `json:"target"`
}

type nftMatch struct {
	Op    string          `json:"op"`
	Left  nftSelector     `json:"left"`
	Right json.RawMessage `json:"right"`
}

type nftSelector struct {
	Meta    *nftMeta    `json:"meta"`
	Payload *nftPayload `json:"payload"`
	Fib     *nftFib     `json:"fib"`
}

type nftMeta struct {
	Key string `json:"key"`
}

type nftPayload struct {
	Protocol string `json:"protocol"`
	Field    string `json:"field"`
}

type nftFib struct {
	Result string          `json:"result"`
	Flags  json.RawMessage `json:"flags"`
}

func matchesScopedFirewall(inventory nftInventory, plan corerunner.Plan) bool {
	type chainObservation struct {
		definition firewallChain
		seen       bool
		rules      int
	}
	table := nftTable(plan)
	comment := scopedFirewallComment(plan)
	chains := make(map[string]*chainObservation, 4)
	for _, chain := range scopedFirewallChains(plan) {
		chains[chain.name] = &chainObservation{definition: chain}
	}
	tables := 0
	for _, entry := range inventory.Entries {
		switch {
		case entry.Table != nil:
			tables++
			if entry.Table.Family != "inet" || entry.Table.Name != table || entry.Table.Comment != comment ||
				len(entry.Table.Flags) != 0 {
				return false
			}
		case entry.Chain != nil:
			chain := entry.Chain
			want := chains[chain.Name]
			if want == nil || want.seen || chain.Table != table || chain.Family != "inet" ||
				!chain.matches(want.definition) {
				return false
			}
			want.seen = true
		case entry.Rule != nil:
			rule := entry.Rule
			want := chains[rule.Chain]
			if want == nil || rule.Table != table || rule.Family != "inet" ||
				want.rules >= len(want.definition.rules) || rule.normalized() != want.definition.rules[want.rules] {
				return false
			}
			want.rules++
		case entry.Metainfo != nil:
		default:
			return false
		}
	}
	for _, chain := range chains {
		if !chain.seen || chain.rules != len(chain.definition.rules) {
			return false
		}
	}
	return tables == 1
}

func (chain nftChainState) matches(want firewallChain) bool {
	if want.hook == "" {
		return chain.Hook == "" && chain.Type == "" && chain.Policy == ""
	}
	return chain.Hook == want.hook && chain.Type == "filter" && chain.Prio == 0 && chain.Policy == "accept"
}

func (rule nftRuleState) normalized() string {
	if len(rule.Expr) < 2 {
		return ""
	}
	var fragments []string
	for _, statement := range rule.Expr[:len(rule.Expr)-1] {
		if statement.Match == nil || statement.Jump != nil || statement.Accept != nil || statement.Drop != nil {
			return ""
		}
		part := statement.Match.normalized()
		if part == "" {
			return ""
		}
		fragments = append(fragments, part)
	}
	verdict := rule.Expr[len(rule.Expr)-1].verdict()
	if verdict == "" {
		return ""
	}
	return strings.Join(append(fragments, verdict), " ")
}

func (statement nftStatement) verdict() string {
	switch {
	case statement.Match != nil:
		return ""
	case statement.Jump != nil && statement.Accept == nil && statement.Drop == nil:
		return "jump " + statement.Jump.Target
	case statement.Jump == nil && string(statement.Accept) == "null" && statement.Drop == nil:
		return "accept"
	case statement.Jump == nil && statement.Accept == nil && string(statement.Drop) == "null":
		return "drop"
	default:
		return ""
	}
}

func (match nftMatch) normalized() string {
	if match.Op != "==" {
		return ""
	}
	left := match.Left
	switch {
	case left.Meta != nil && left.Payload == nil && left.Fib == nil:
		return match.meta()
	case left.Payload != nil && left.Meta == nil && left.Fib == nil:
		return match.payload()
	case left.Fib != nil && left.Meta == nil && left.Payload == nil:
		if left.Fib.Result == "type" && left.Fib.destinationOnly() &&
			(nftString(match.Right) == "local" || nftUint(match.Right) == "2") {
			return "fib daddr type local"
		}
	}
	return ""
}

func (fib nftFib) destinationOnly() bool {
	if nftString(fib.Flags) == "daddr" {
		return true
	}
	var flags []string
	return json.Unmarshal(fib.Flags, &flags) == nil && len(flags) == 1 && flags[0] == "daddr"
}

func (match nftMatch) meta() string {
	switch match.Left.Meta.Key {
	case "skuid":
		if number := nftUint(match.Right); number != "" {
			return "meta skuid " + number
		}
	case "iifname":
		if value := nftString(match.Right); value != "" {
			return "meta iifname " + value
		}
	case "nfproto":
		if nftString(match.Right) == "ipv4" || nftUint(match.Right) == "2" {
			return "meta nfproto ipv4"
		}
		if nftString(match.Right) == "ipv6" || nftUint(match.Right) == "10" {
			return "meta nfproto ipv6"
		}
	}
	return ""
}

func (match nftMatch) payload() string {
	payload := match.Left.Payload
	switch {
	case payload.Protocol == "ip" && payload.Field == "daddr":
		if address := nftAddress(match.Right); address != "" {
			return "ip daddr " + address
		}
	case (payload.Protocol == "tcp" || payload.Protocol == "udp") && payload.Field == "dport":
		if port := nftUint(match.Right); port != "" {
			return payload.Protocol + " dport " + port
		}
	}
	return ""
}

func nftString(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}

func nftUint(raw json.RawMessage) string {
	value := string(raw)
	if label := nftString(raw); label != "" {
		value = label
	}
	if number, err := strconv.ParseUint(value, 10, 32); err == nil {
		return strconv.FormatUint(number, 10)
	}
	return ""
}

func nftAddress(raw json.RawMessage) string {
	if ip, err := netip.ParseAddr(nftString(raw)); err == nil && ip.Is4() {
		return ip.String()
	}
	var value struct {
		Prefix *struct {
			Addr string `json:"addr"`
			Len  int    `json:"len"`
		} `json:"prefix"`
	}
	if err := json.Unmarshal(raw, &value); err != nil || value.Prefix == nil {
		return ""
	}
	addr, err := netip.ParseAddr(value.Prefix.Addr)
	prefix := netip.PrefixFrom(addr, value.Prefix.Len)
	if err == nil && addr.Is4() && prefix.IsValid() {
		return addressOrPrefix(prefix.Masked())
	}
	return ""
}
