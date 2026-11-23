import type { ReactNode } from "react";
import { EmptyState } from "./empty-state";

export function ResourceLookupState({
  resource,
  loading,
  error,
  description,
  icon,
  action,
}: {
  resource: string;
  loading?: boolean;
  error?: string | null;
  description?: string;
  icon: ReactNode;
  action?: ReactNode;
}) {
  return (
    <EmptyState
      icon={icon}
      loading={loading}
      error={!loading && !!error}
      title={
        loading
          ? `Loading ${resource}`
          : error
            ? `Unable to load ${resource}`
            : `${resource} not found`
      }
      description={loading ? undefined : error ?? description}
      action={loading ? undefined : action}
    />
  );
}
