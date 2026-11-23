import { CopyButton } from "@/components/common/copy-button";
import { RevealValue } from "@/components/common/reveal-value";
import { useEffect, useState } from "react";
import { revealAttachFactValue } from "./api";

type FactIdentity = {
  attachId: string;
  grantAttachId?: string;
  k: string;
};

export function FactRow({
  attachId,
  grantAttachId,
  k,
  secret,
}: FactIdentity & { secret: boolean }) {
  const identity = `${attachId}/${grantAttachId ?? ""}/${k}`;
  return (
    <div className="grid min-w-0 grid-cols-[minmax(0,min(10rem,45%))_minmax(0,1fr)] items-center gap-2">
      <span className="min-w-0 font-mono text-xs text-muted-foreground [overflow-wrap:anywhere]">
        {k}
      </span>
      {secret ? (
        <RevealValue
          key={identity}
          label={k}
          loadValue={() => revealAttachFactValue(attachId, k, grantAttachId)}
        />
      ) : (
        <PublicFactValue
          key={identity}
          attachId={attachId}
          grantAttachId={grantAttachId}
          k={k}
        />
      )}
    </div>
  );
}

function PublicFactValue({ attachId, grantAttachId, k }: FactIdentity) {
  const [value, setValue] = useState<string>();
  const [unavailable, setUnavailable] = useState(false);
  useEffect(() => {
    const controller = new AbortController();
    void revealAttachFactValue(
      attachId,
      k,
      grantAttachId,
      controller.signal,
    ).then(
      (result) => {
        if (!controller.signal.aborted) setValue(result);
      },
      () => {
        if (!controller.signal.aborted) setUnavailable(true);
      },
    );
    return () => controller.abort();
  }, [attachId, grantAttachId, k]);

  return (
    <div className="flex min-w-0 items-center gap-1">
      <code
        className="h-9 min-w-0 flex-1 overflow-x-auto rounded-md bg-muted px-3 py-2 font-mono text-sm whitespace-pre"
        aria-label={k}
        aria-busy={value === undefined && !unavailable}
      >
        {value || (
          <span className="font-sans text-xs text-muted-foreground">
            {value === ""
              ? "Empty value"
              : unavailable
                ? "Unavailable"
                : "Loading…"}
          </span>
        )}
      </code>
      {value === undefined ? (
        <span className="size-8 shrink-0" aria-hidden />
      ) : (
        <CopyButton
          className="size-8 shrink-0"
          value={value}
          label={`Copy ${k}`}
          iconOnly
        />
      )}
    </div>
  );
}
