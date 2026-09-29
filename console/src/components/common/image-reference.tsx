export function ImageReference({ value }: { value: string }) {
  const digest = value.match(/((sha256:|sha-)[a-f0-9]{64})$/);
  const label = digest
    ? value.replace(digest[1], digest[1].slice(0, digest[2].length + 12))
    : value;
  return (
    <span title={value} className="block min-w-0 truncate text-xs">
      {label || "No image configured"}
    </span>
  );
}
