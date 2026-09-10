// An empty or fractional input must not become a fabricated zero/default.
export function parseScriptOrder(value: string): number | undefined {
  if (!/^\d{1,5}$/.test(value)) return undefined
  const order = Number(value)
  return order <= 65535 ? order : undefined
}
