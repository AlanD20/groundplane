import type { HostInfo } from '../platform-host/host-model'

export type ControllerRelease = NonNullable<HostInfo['controller']['update']['candidate']>

export function ControllerReleaseDetails({ release }: { release: ControllerRelease }) {
  return (
    <dl className="grid min-w-0 gap-3 rounded-lg border border-border bg-surface p-3 text-xs sm:grid-cols-2">
      <Field label="Controller version" value={release.controller_version} />
      <Field label="Compatibility" value={`Storage epoch ${release.storage_epoch} · Channel schema ${release.channel_schema}`} />
      <Field label="Release manifest" value={release.release} wide />
      <Field label="Controller binary" value={release.controller_sha256} wide />
      <Field label="Agent image" value={release.agent_image} wide />
    </dl>
  )
}

function Field({ label, value, wide }: { label: string; value: string; wide?: boolean }) {
  return (
    <div className={wide ? 'min-w-0 sm:col-span-2' : 'min-w-0'}>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-all font-mono">{value}</dd>
    </div>
  )
}
