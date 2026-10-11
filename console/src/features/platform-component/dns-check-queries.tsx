import { useState } from "react";
import { Plus, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Disclosure } from "@/components/common/disclosure";
import type { DNSCheckQuery } from "./dns-check-api";

export function DNSCheckQueries({
  domains,
  queries,
  onChange,
  disabled,
}: {
  domains: string[];
  queries: DNSCheckQuery[];
  onChange: (queries: DNSCheckQuery[]) => void;
  disabled: boolean;
}) {
  const [domain, setDomain] = useState(".");
  const [name, setName] = useState("");
  const [type, setType] = useState<DNSCheckQuery["type"]>("A");
  const available = [...new Set([".", ...domains])];
  const canonicalDomain = (value: string) =>
    value === "." ? value : `${value.toLowerCase().replace(/\.$/, "")}.`;
  const used = new Set(queries.map((query) => canonicalDomain(query.domain)));
  const options = available
    .filter((value) => !used.has(canonicalDomain(value)))
    .map((value) => ({
      value,
      label: value === "." ? "Default upstream" : value,
    }));
  const selected = options.some((option) => option.value === domain)
    ? domain
    : (options[0]?.value ?? "");
  function add() {
    if (disabled || !selected || !name.trim() || queries.length >= 10) return;
    onChange([...queries, { domain: selected, name: name.trim(), type }]);
    setName("");
  }
  return (
    <Disclosure
      summary={
        <span className="text-xs font-medium">
          Choose query names (optional)
        </span>
      }
    >
      <div className="space-y-3 pt-3">
        <p className="text-xs text-muted-foreground">
          Defaults: root NS for the default upstream and an A query for each
          forwarding domain. Override a name within its selected domain; servers
          still come from the applied configuration.
        </p>
        <div className="grid grid-cols-1 items-end gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_5rem_auto]">
          <label className="space-y-1 text-xs">
            Forwarding domain
            <Select
              aria-label="Forwarding domain to test"
              value={selected}
              options={options}
              onValueChange={setDomain}
              disabled={disabled || !options.length}
            />
          </label>
          <label className="space-y-1 text-xs">
            Name
            <Input
              aria-label="DNS query name"
              placeholder="api.example.internal"
              value={name}
              onChange={(event) => setName(event.target.value)}
              disabled={disabled}
              maxLength={253}
            />
          </label>
          <label className="space-y-1 text-xs">
            Type
            <Select
              aria-label="DNS query type"
              value={type}
              options={[
                { value: "A", label: "A" },
                { value: "NS", label: "NS" },
              ]}
              onValueChange={(value) => {
                if (value === "A" || value === "NS") setType(value);
              }}
              disabled={disabled}
            />
          </label>
          <Button
            type="button"
            variant="outline"
            size="icon"
            aria-label="Add query override"
            disabled={
              disabled || !selected || !name.trim() || queries.length >= 10
            }
            onClick={add}
          >
            <Plus className="size-4" />
          </Button>
        </div>
        {queries.length > 0 && (
          <ul className="space-y-2">
            {queries.map((query, index) => (
              <li
                key={query.domain}
                className="flex items-center justify-between gap-2 rounded-lg border border-border px-3 py-2 text-xs"
              >
                <span className="min-w-0 break-all">
                  {query.name} ({query.type})
                  <span className="ml-2 text-muted-foreground">
                    {query.domain === "." ? "Default upstream" : query.domain}
                  </span>
                </span>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-xs"
                  aria-label={`Remove query for ${query.domain}`}
                  disabled={disabled}
                  onClick={() =>
                    onChange(
                      queries.filter((_, candidate) => candidate !== index),
                    )
                  }
                >
                  <X className="size-3.5" />
                </Button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </Disclosure>
  );
}
