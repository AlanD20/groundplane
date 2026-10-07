import { useState } from "react";
import { CopyButton } from "./copy-button";

/** Short labels are presentation only; the complete reference remains selectable. */
export function CompactReference({
  value,
  label = "ID",
  short,
  hideLabel = false,
}: {
  value: string;
  label?: string;
  short?: string;
  hideLabel?: boolean;
}) {
  const [expanded, setExpanded] = useState(false);
  const abbreviated =
    short ??
    (value.length > 24 ? `${value.slice(0, 8)}…${value.slice(-8)}` : value);
  return (
    <span className="inline-flex min-w-0 max-w-full flex-col items-start font-sans text-xs">
      <span className="inline-flex min-w-0 max-w-full items-center gap-1">
        <button
          type="button"
          aria-label={`Show full ${label}`}
          aria-expanded={expanded}
          onClick={() => setExpanded(!expanded)}
          className="min-w-0 truncate rounded px-1 py-1 text-muted-foreground hover:bg-muted hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring"
        >
          {!hideLabel && <>{label} </>}
          <span className="font-mono">{abbreviated}</span>
        </button>
        <CopyButton
          value={value}
          label={`Copy ${label}`}
          iconOnly
          className="shrink-0"
        />
      </span>
      {expanded && (
        <code className="max-w-full select-all break-all rounded bg-muted px-2 py-1 text-foreground">
          {value}
        </code>
      )}
    </span>
  );
}
