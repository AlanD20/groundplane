function address(value: number): string {
  return [24, 16, 8, 0].map((shift) => Math.floor(value / 2 ** shift) % 256).join('.')
}

function bounds(cidr: string): { start: string; end: string } | null {
  const match = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})\/(\d{1,2})$/.exec(cidr.trim())
  if (!match) return null
  const octets = match.slice(1, 5).map(Number)
  const prefix = Number(match[5])
  if (octets.some((octet) => octet > 255) || prefix > 32) return null
  const size = 2 ** (32 - prefix)
  const start = Math.floor(octets.reduce((value, octet) => value * 256 + octet, 0) / size) * size
  return { start: address(start), end: address(start + size - 1) }
}

/** Display-only CIDR bounds, not an allocation or availability decision. */
export function NetworkRange({ cidr, label = 'Pool' }: { cidr: string; label?: string }) {
  const range = bounds(cidr)
  if (!range) return null
  return (
    <div className="space-y-2 rounded-lg bg-surface p-3 text-xs sm:col-span-2" aria-live="polite">
      <span className="font-medium">
        {label}: <code>{cidr}</code>
      </span>
      <dl className="grid gap-2 sm:grid-cols-2">
        <div>
          <dt className="text-muted-foreground">Start address</dt>
          <dd className="font-mono">{range.start}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">End address</dt>
          <dd className="font-mono">{range.end}</dd>
        </div>
      </dl>
      <p className="text-muted-foreground">
        Full CIDR bounds, not free addresses. Existing zones and reserved addresses still apply.
      </p>
    </div>
  )
}
