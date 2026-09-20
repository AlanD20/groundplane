'use client'

import { FileCode2 } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useId } from 'react'
import { CodeEditor } from '@/components/ui/code-editor'

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
  const id = useId()
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2 text-sm">
          <FileCode2 className="size-4 text-muted-foreground" />
          {title}
        </CardTitle>
        <span className="flex items-center gap-2">
          <span className="font-mono text-xs text-muted-foreground">{path}</span>
        </span>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <CodeEditor id={id} label={title} value={content} language={/\.ya?ml$/.test(path) ? 'yaml' : path.endsWith('.json') ? 'json' : 'text'} readOnly />
        {note ? <p className="text-xs text-muted-foreground">{note}</p> : null}
      </CardContent>
    </Card>
  )
}
