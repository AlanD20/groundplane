'use client'

import { useMemo, useRef, useState, type ChangeEvent } from 'react'
import { ArrowDown, ArrowUp, FileArchive, FolderOpen, Plus, Trash2, Upload } from 'lucide-react'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useStore } from '@/lib/store'
import type { Environment } from '@/lib/types'
import {
  BLUEPRINT_BUNDLE_LIMITS,
  createBlueprintApplyRequest,
  formatBlueprintBytes,
  inspectBlueprintFiles,
  validateBlueprintSelection,
  type BlueprintApplyRequest,
  type BlueprintInterpolation,
  type InspectedBlueprintFile,
} from '@/lib/blueprint-bundle'

type InterpolationEntry = BlueprintInterpolation & {
  id: number
}

export function BlueprintApplyAction({ environment, workspace }: { environment: Environment; workspace: string }) {
  const store = useStore()
  const [open, setOpen] = useState(false)
  const [files, setFiles] = useState<InspectedBlueprintFile[]>([])
  const [rootPath, setRootPath] = useState('')
  const [composeSources, setComposeSources] = useState<string[]>([])
  const [sourceCandidate, setSourceCandidate] = useState('')
  const [entries, setEntries] = useState<InterpolationEntry[]>([])
  const entrySequence = useRef(0)
  const [reading, setReading] = useState(false)
  const [readError, setReadError] = useState('')
  const [prepared, setPrepared] = useState<BlueprintApplyRequest | null>(null)
  const [completed, setCompleted] = useState(false)

  const sortedFiles = files
  const totalBytes = files.reduce((sum, entry) => sum + entry.size, 0)
  const errors = useMemo(
    () => validateBlueprintSelection(files, rootPath, composeSources, entries),
    [composeSources, entries, files, rootPath],
  )

  async function selectFiles(event: ChangeEvent<HTMLInputElement>, directorySelection: boolean) {
    const selected = Array.from(event.currentTarget.files ?? [])
    event.currentTarget.value = ''
    setReading(true)
    setReadError('')
    setRootPath('')
    setComposeSources([])
    setSourceCandidate('')
    try {
      setFiles(await inspectBlueprintFiles(selected, directorySelection))
    } catch {
      setFiles([])
      setReadError('The browser could not read the selected files.')
    } finally {
      setReading(false)
    }
  }

  function changeRoot(path: string) {
    setRootPath(path)
    setComposeSources((current) => current.filter((source) => source !== path))
    if (sourceCandidate === path) setSourceCandidate('')
  }

  function addSource() {
    if (!sourceCandidate || sourceCandidate === rootPath || composeSources.includes(sourceCandidate)) return
    setComposeSources((current) => [...current, sourceCandidate])
    setSourceCandidate('')
  }

  function moveSource(index: number, offset: -1 | 1) {
    setComposeSources((current) => {
      const target = index + offset
      if (target < 0 || target >= current.length) return current
      const next = [...current]
      ;[next[index], next[target]] = [next[target], next[index]]
      return next
    })
  }

  function addEntry() {
    entrySequence.current += 1
    setEntries((current) => [...current, { id: entrySequence.current, key: '', value: '' }])
  }

  function prepareApply() {
    if (errors.length > 0 || reading) return
    setCompleted(false)
    setPrepared(createBlueprintApplyRequest(files, rootPath, composeSources, entries))
  }

  function reset() {
    setFiles([])
    setRootPath('')
    setComposeSources([])
    setSourceCandidate('')
    setEntries([])
    entrySequence.current = 0
    setReadError('')
    setPrepared(null)
    setCompleted(false)
  }

  function closeEditor() {
    setOpen(false)
    reset()
  }

  const inputOptions = sortedFiles.map((entry) => ({ value: entry.path, label: entry.path }))
  const sourceOptions = inputOptions.filter((option) => option.value !== rootPath && !composeSources.includes(option.value))

  return (
    <>
      <Button variant="outline" size="sm" onClick={() => setOpen(true)}>
        <Upload /> Apply Blueprint
      </Button>
      <Drawer open={open && !prepared} onOpenChange={(next) => { if (next) setOpen(true); else closeEditor() }}>
        <DrawerContent className="max-w-3xl">
          <DialogHeader className="pr-8">
            <DialogTitle className="flex items-center gap-2"><FileArchive className="size-5 text-primary" /> Apply Blueprint</DialogTitle>
            <DialogDescription>
              Submit one closed bundle. The root is always the first Compose source; additional sources apply in the order shown.
            </DialogDescription>
          </DialogHeader>

          {environment.lastAppliedBlueprint && (
            <div className="rounded-lg border border-border bg-surface px-3 py-2 text-xs text-muted-foreground">
              Last applied generation <span className="font-mono text-foreground">{environment.lastAppliedBlueprint.generation}</span>
              {' · '}root <span className="font-mono text-foreground">{environment.lastAppliedBlueprint.rootPath}</span>
              {' · '}{environment.lastAppliedBlueprint.appliedAt}
            </div>
          )}

          <section className="space-y-3 rounded-xl border border-border bg-card p-4">
            <div>
              <h3 className="text-sm font-medium">1. Closed file namespace</h3>
              <p className="text-xs text-muted-foreground">Choose a directory when supported, or use the portable multi-file fallback.</p>
            </div>
            <div className="grid gap-2 sm:grid-cols-2">
              <Label className="flex h-9 cursor-pointer items-center justify-center gap-2 rounded-lg border border-input bg-background px-3 hover:bg-muted/50">
                <FolderOpen className="size-4" /> Select directory
                <input
                  id="blueprint-directory"
                  name="blueprint-directory"
                  className="sr-only"
                  type="file"
                  multiple
                  {...({ webkitdirectory: '' } as React.InputHTMLAttributes<HTMLInputElement>)}
                  onChange={(event) => void selectFiles(event, true)}
                />
              </Label>
              <Label className="flex h-9 cursor-pointer items-center justify-center gap-2 rounded-lg border border-input bg-background px-3 hover:bg-muted/50">
                <FileArchive className="size-4" /> Select files instead
                <input id="blueprint-files" name="blueprint-files" className="sr-only" type="file" multiple onChange={(event) => void selectFiles(event, false)} />
              </Label>
            </div>
            <div className="flex flex-wrap gap-2 text-xs">
              <Badge variant="outline">{files.length} / {BLUEPRINT_BUNDLE_LIMITS.files} files</Badge>
              <Badge variant="outline">{formatBlueprintBytes(totalBytes)} / {formatBlueprintBytes(BLUEPRINT_BUNDLE_LIMITS.totalBytes)}</Badge>
              <Badge variant="outline">{formatBlueprintBytes(BLUEPRINT_BUNDLE_LIMITS.fileBytes)} per file</Badge>
              <Badge variant="outline">{BLUEPRINT_BUNDLE_LIMITS.pathBytes}-byte paths</Badge>
            </div>
            {reading && <p role="status" className="text-xs text-info">Reading files and calculating SHA-256 digests…</p>}
            {readError && <p role="alert" className="text-xs text-destructive">{readError}</p>}
          </section>

          <section className="space-y-3 rounded-xl border border-border bg-card p-4">
            <div>
              <h3 className="text-sm font-medium">2. Ordered Compose sources</h3>
              <p className="text-xs text-muted-foreground">Select the Groundplane envelope root explicitly. It cannot be moved from position 01.</p>
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="blueprint-root">Root Blueprint</Label>
              <select id="blueprint-root" name="blueprint-root" className="h-8 w-full rounded-lg border border-input bg-background px-2.5 text-sm" value={rootPath} onChange={(event) => changeRoot(event.target.value)}>
                <option value="">Select the root file…</option>
                {inputOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
            </div>
            {rootPath && <div className="flex items-center gap-2 rounded-lg border border-primary/30 bg-primary/5 px-3 py-2 text-sm"><span className="font-mono text-xs text-muted-foreground">01</span><span className="min-w-0 truncate font-mono">{rootPath}</span><Badge className="ml-auto" variant="outline">root</Badge></div>}
            {composeSources.map((source, index) => (
              <div key={source} className="flex items-center gap-2 rounded-lg border border-border bg-background px-3 py-2 text-sm">
                <span className="font-mono text-xs text-muted-foreground">{String(index + 2).padStart(2, '0')}</span>
                <span className="min-w-0 flex-1 truncate font-mono">{source}</span>
                <Button aria-label={`Move ${source} up`} variant="ghost" size="icon-xs" disabled={index === 0} onClick={() => moveSource(index, -1)}><ArrowUp /></Button>
                <Button aria-label={`Move ${source} down`} variant="ghost" size="icon-xs" disabled={index === composeSources.length - 1} onClick={() => moveSource(index, 1)}><ArrowDown /></Button>
                <Button aria-label={`Remove ${source} source`} variant="ghost" size="icon-xs" onClick={() => setComposeSources((current) => current.filter((candidate) => candidate !== source))}><Trash2 /></Button>
              </div>
            ))}
            <div className="flex flex-col gap-2 sm:flex-row">
              <select id="blueprint-compose-source" name="blueprint-compose-source" aria-label="Additional Compose source" className="h-8 min-w-0 flex-1 rounded-lg border border-input bg-background px-2.5 text-sm" value={sourceCandidate} onChange={(event) => setSourceCandidate(event.target.value)}>
                <option value="">Select an additional source…</option>
                {sourceOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
              <Button variant="outline" disabled={!sourceCandidate} onClick={addSource}><Plus /> Add source</Button>
            </div>
          </section>

          <section className="space-y-3 rounded-xl border border-border bg-card p-4">
            <div className="flex items-start justify-between gap-3">
              <div><h3 className="text-sm font-medium">3. Non-secret interpolation</h3><p className="text-xs text-muted-foreground">Explicit values only. Never enter credentials or secret material here.</p></div>
              <Button variant="outline" size="sm" onClick={addEntry}><Plus /> Variable</Button>
            </div>
            {entries.map((entry) => (
              <div key={entry.id} className="grid gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1.5fr)_auto]">
                <Input id={`blueprint-var-key-${entry.id}`} name={`blueprint-var-key-${entry.id}`} aria-label="Interpolation key" placeholder="KEY" value={entry.key} onChange={(event) => setEntries((current) => current.map((candidate) => candidate.id === entry.id ? { ...candidate, key: event.target.value } : candidate))} />
                <Input id={`blueprint-var-value-${entry.id}`} name={`blueprint-var-value-${entry.id}`} aria-label={`Value for ${entry.key || 'interpolation key'}`} placeholder="value" value={entry.value} onChange={(event) => setEntries((current) => current.map((candidate) => candidate.id === entry.id ? { ...candidate, value: event.target.value } : candidate))} />
                <Button className="justify-self-end sm:justify-self-auto" aria-label="Remove interpolation variable" variant="ghost" size="icon" onClick={() => setEntries((current) => current.filter((candidate) => candidate.id !== entry.id))}><Trash2 /></Button>
              </div>
            ))}
            {entries.length === 0 && <p className="text-xs text-muted-foreground">No interpolation values supplied.</p>}
          </section>

          <section className="space-y-3 rounded-xl border border-border bg-card p-4">
            <div><h3 className="text-sm font-medium">4. Deterministic manifest preview</h3><p className="text-xs text-muted-foreground">Files are normalized and path-sorted before deterministic part identities are assigned.</p></div>
            <div className="max-h-56 space-y-1 overflow-y-auto rounded-lg border border-border bg-background p-2">
              {sortedFiles.map((entry) => (
                <div key={`${entry.path}-${entry.part}`} className="grid gap-x-3 rounded-md px-2 py-1.5 text-xs sm:grid-cols-[7rem_minmax(0,1fr)_auto]">
                  <code className="text-muted-foreground">{entry.part}</code>
                  <code className="min-w-0 break-all">{entry.path || '(invalid path)'}</code>
                  <span className="text-muted-foreground sm:text-right">{formatBlueprintBytes(entry.size)}</span>
                  <code className="break-all text-[10px] text-muted-foreground sm:col-span-3">SHA-256 {entry.sha256 || 'not computed while bundle limits fail'}</code>
                </div>
              ))}
              {files.length === 0 && <p className="px-2 py-3 text-xs text-muted-foreground">Select files to build the manifest.</p>}
            </div>
            <p className="text-xs text-muted-foreground">Browsers cannot prove symlink safety. The Controller rejects symlinks and validates every include, extends, env_file, label_file, config, secret reference, and bind against the declared closed namespace.</p>
          </section>

          {errors.length > 0 && files.length > 0 && <div role="alert" className="rounded-lg border border-destructive/30 bg-destructive/5 p-3"><p className="mb-1 text-xs font-medium text-destructive">Resolve before applying</p><ul className="space-y-1 text-xs text-destructive">{errors.map((error) => <li key={error}>· {error}</li>)}</ul></div>}

          <div className="flex justify-end gap-2 border-t border-border pt-4">
            <Button variant="outline" onClick={closeEditor}>Cancel</Button>
            <Button disabled={reading || errors.length > 0} onClick={prepareApply}><Upload /> Review Apply task</Button>
          </div>
        </DrawerContent>
      </Drawer>

      {prepared && (
        <TaskRunnerDialog
          open
          onOpenChange={(next) => {
            if (next) return
            if (completed) closeEditor()
            else setPrepared(null)
          }}
          title={`Apply Blueprint · ${environment.name}`}
          description="Validate and atomically commit the closed Blueprint bundle, then schedule reconciliation."
          type="update"
          target={environment.id}
          workspace={workspace}
          executionCopy="Controller validates the closed namespace and commits normalized desired state before the Agent receives reconciliation work:"
          steps={[
            { label: 'Verify paths, sizes, file parts, and SHA-256 digests', state: 'pending' },
            { label: 'Parse the root and ordered Compose sources', state: 'pending' },
            { label: 'Atomically commit normalized desired state', state: 'pending' },
            { label: 'Schedule environment reconciliation', state: 'pending' },
          ]}
          review={<div className="rounded-lg border border-border bg-surface p-3 text-xs"><p><span className="text-muted-foreground">Root </span><code>{prepared.manifest.root}</code></p><p><span className="text-muted-foreground">Sources </span>{prepared.manifest.compose_sources.length}</p><p><span className="text-muted-foreground">Files </span>{prepared.manifest.files.length} · {formatBlueprintBytes(prepared.manifest.files.reduce((sum, file) => sum + file.size, 0))}</p><p><span className="text-muted-foreground">Interpolation keys </span>{Object.keys(prepared.manifest.interpolation).join(', ') || 'none'}</p></div>}
          startLabel="Apply Blueprint"
          onDispatch={async () => {
            const accepted = await store.applyBlueprint(environment.id, prepared)
            setCompleted(true)
            return accepted.task_id
          }}
          variant="drawer"
        />
      )}
    </>
  )
}
