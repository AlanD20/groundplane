'use client'

import { FileCode2 } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { CopyButton } from '@/components/common/copy-button'

// The Controller-rendered config file for a platform component — the full
// file as written, exactly like the desired-state (Blueprint) views. What
// you see here is what the Agent applies.
export function ConfigView({
  path,
  title = 'Config',
  content,
  note,
}: {
  path: string
  title?: string
  content: string
  note?: string
}) {
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2 text-sm">
          <FileCode2 className="size-4 text-muted-foreground" />
          {title}
        </CardTitle>
        <span className="flex items-center gap-2">
          <span className="font-mono text-xs text-muted-foreground">{path}</span>
          <CopyButton value={content} label="copy" />
        </span>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <pre className="overflow-x-auto rounded-lg border border-border bg-surface p-3 text-xs leading-relaxed text-muted-foreground">
          <code>{content}</code>
        </pre>
        {note ? <p className="text-xs text-muted-foreground">{note}</p> : null}
      </CardContent>
    </Card>
  )
}
