import type { EnvironmentDeletionFailure } from '@/features/environment/environment-removal-model'
import { EnvironmentDeletionFailureNotice } from './environment-deletion-failure'

type EnvironmentDeletionFenceProps = {
  inProgress: boolean
  failure: EnvironmentDeletionFailure | null
  onRetry: () => Promise<unknown>
  retryLabel: string
}

export function EnvironmentDeletionFence({ inProgress, failure, onRetry, retryLabel }: EnvironmentDeletionFenceProps) {
  if (!inProgress && !failure) return null
  return (
    <div className="flex flex-col gap-2 rounded-lg border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning" role="status">
      {inProgress && <p>Environment deletion is in progress. Editing, deployment, and child mutations are disabled until the Controller completes the deletion.</p>}
      {failure && <EnvironmentDeletionFailureNotice failure={failure} onRetry={onRetry} retryLabel={retryLabel} />}
    </div>
  )
}
