import { useState } from "react";
import { Button } from "@/components/ui/button";
import type { EnvironmentDeletionFailure } from "@/features/environment/environment-removal-model";

type EnvironmentDeletionFailureNoticeProps = {
  failure: EnvironmentDeletionFailure;
  onRetry: () => Promise<unknown>;
  retryLabel: string;
};

export function EnvironmentDeletionFailureNotice({
  failure,
  onRetry,
  retryLabel,
}: EnvironmentDeletionFailureNoticeProps) {
  const [retrying, setRetrying] = useState(false);
  const [error, setError] = useState<string | null>(null);

  return (
    <div className="flex flex-col gap-2 rounded-lg border border-destructive/30 bg-destructive/5 px-3 py-2">
      <p className="text-xs text-destructive" role="alert">
        {failure.message}
      </p>
      {error && (
        <p className="text-xs text-destructive" role="alert">
          {error}
        </p>
      )}
      <Button
        variant="outline"
        size="sm"
        className="self-start"
        disabled={retrying}
        onClick={async () => {
          setRetrying(true);
          setError(null);
          try {
            await onRetry();
          } catch (reason) {
            setError(
              reason instanceof Error
                ? reason.message
                : "Unable to retry Environment deletion",
            );
          } finally {
            setRetrying(false);
          }
        }}
      >
        {retrying ? "Retrying…" : retryLabel}
      </Button>
    </div>
  );
}
