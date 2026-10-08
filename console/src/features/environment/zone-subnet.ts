/** Input guidance only; the Controller owns subnet admission and allocation. */
export function zoneSubnetError(value: string): string | null {
  const subnet = value.trim();
  if (!subnet) return "Subnet is required.";
  const match = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})\/(\d{1,2})$/.exec(
    subnet,
  );
  if (
    !match ||
    match.slice(1, 5).some((octet) => Number(octet) > 255) ||
    Number(match[5]) > 32
  ) {
    return "Enter an IPv4 subnet, for example 10.35.0.240/28.";
  }
  return Number(match[5]) > 30
    ? "No usable container addresses. Use /30 or a larger network; /28 is recommended."
    : null;
}
