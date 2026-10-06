import type { ActivityEntry } from '@/lib/types'
import { CopyButton } from '@/components/common/copy-button'
import { TaskJournalMetadata } from '@/components/common/task-journal-metadata'
import { imageSize } from './api'

export function ImageFetchTaskDetails({ task }: { task: ActivityEntry }) {
  const fetch = task.imageFetch
  if (!fetch) return null
  const progress = fetch.progress
  const active = task.status === 'pending' || task.status === 'running'
  const phases: Record<string, string> = {
    checking: 'Checking whether the selected image is already on this host',
    downloading: 'Downloading image layers to this host',
    verifying: 'Verifying the downloaded image against the selected registry content',
    verified: 'Image verified; finishing the Task',
  }
  const summary = task.status === 'completed' ? 'Image is available on this host. You can now select it for a Service and Deploy.'
    : task.status === 'pending' ? 'Queued — waiting for the Controller to start the fetch.'
    : task.status === 'failed' ? 'Image fetch failed. No Service was changed or deployed.'
    : task.status === 'aborted' ? 'Image fetch was aborted. No Service was changed or deployed.'
    : task.status === 'timed_out' ? 'Image fetch reached its time limit. No Service was changed or deployed.'
    : phases[progress.phase ?? ''] ?? 'Fetching the selected image. Waiting for progress from the Controller.'
  return <div className="space-y-4">
    <section aria-live="polite" className="space-y-2 rounded-lg border border-border bg-surface p-4">
      <p className="text-sm font-medium">{summary}</p>
      <p className="text-xs text-muted-foreground">Platform: {fetch.platform}</p>
      {active && (progress.total_bytes ?? 0) > 0 && <div className="space-y-1">
        <p className="text-sm">{imageSize(progress.downloaded_bytes ?? 0)} / {imageSize(progress.total_bytes ?? 0)} downloaded</p>
        <p className="text-xs text-muted-foreground">Reported layers only; the total can grow as Docker discovers more layers. Cached layers are not downloaded again.</p>
      </div>}
      {!active && progress.phase && task.status !== 'completed' && <p className="text-xs text-muted-foreground">Last stage: {phases[progress.phase] ?? progress.phase}</p>}
    </section>
    {progress.error_detail && <div role="alert" className="space-y-2 rounded-lg border border-destructive/40 bg-destructive/5 p-4 text-sm">
      <p className="font-medium text-destructive">{progress.error_detail}</p>
      {progress.error_code && <p className="text-xs text-muted-foreground">Error code: {progress.error_code}</p>}
      <p className="text-xs text-muted-foreground">Retry uses the same selected image digest. To select a newer version of the tag, start a new Fetch.</p>
    </div>}
    {task.status === 'failed' && !progress.error_detail && <p className="text-sm text-muted-foreground">No failure explanation was recorded for this attempt. The Controller log contains its diagnostic.</p>}
    <TaskJournalMetadata entry={task} />
    <details className="rounded-lg border border-border p-3">
      <summary className="cursor-pointer text-sm font-medium">Image digest & Task identifiers</summary>
      <dl className="mt-3 space-y-3 text-xs">
        {[['Requested image', fetch.requested], ['Selected digest', fetch.image], ['Task', task.id], ['Operation', task.operationId]].map(([label, value]) => value && <div key={label}>
          <dt className="mb-1 text-muted-foreground">{label}</dt>
          <dd className="flex min-w-0 items-start gap-2"><code className="min-w-0 flex-1 break-all">{value}</code><CopyButton value={value} label={`Copy ${label?.toLowerCase()}`} /></dd>
        </div>)}
      </dl>
    </details>
  </div>
}
