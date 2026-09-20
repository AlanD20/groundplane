import { useEffect, useRef, useState } from 'react'
import { Compartment, EditorState } from '@codemirror/state'
import { EditorView, keymap, lineNumbers, highlightActiveLineGutter, drawSelection } from '@codemirror/view'
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands'
import { HighlightStyle, syntaxHighlighting, StreamLanguage } from '@codemirror/language'
import { tags } from '@lezer/highlight'
import { yaml } from '@codemirror/lang-yaml'
import { json } from '@codemirror/lang-json'
import { shell } from '@codemirror/legacy-modes/mode/shell'
import { Button } from './button'
import { cn } from '@/lib/utils'

export type CodeLanguage = 'yaml' | 'json' | 'shell' | 'text'

type CodeEditorProps = {
  id: string
  label: string
  value: string
  onValueChange?: (value: string) => void
  language?: CodeLanguage
  readOnly?: boolean
  disabled?: boolean
  className?: string
  'aria-describedby'?: string
  'aria-invalid'?: boolean
}

const editorTheme = EditorView.theme({
  '&': { backgroundColor: 'var(--background)', color: 'var(--foreground)', fontSize: '12px' },
  '&.cm-focused': { outline: 'none' },
  '.cm-scroller': { fontFamily: 'var(--font-mono)', lineHeight: '1.8', overflow: 'auto' },
  '.cm-content': { minHeight: '240px', padding: '12px 0', caretColor: 'var(--primary)' },
  '.cm-gutters': {
    backgroundColor: 'var(--card)',
    color: 'var(--muted-foreground)',
    borderRight: '1px solid var(--border)',
  },
  '.cm-activeLineGutter': { backgroundColor: 'var(--accent)', color: 'var(--primary)' },
  '.cm-line': { padding: '0 16px' },
  '&.cm-focused .cm-selectionBackground, .cm-selectionBackground, ::selection': {
    backgroundColor: 'color-mix(in srgb, var(--primary) 22%, transparent)',
  },
  '.cm-cursor': { borderLeftColor: 'var(--primary)' },
})

const highlightStyle = HighlightStyle.define([
  { tag: [tags.keyword, tags.propertyName, tags.typeName], color: 'var(--primary)' },
  { tag: [tags.string, tags.special(tags.string)], color: 'var(--success)' },
  { tag: [tags.number, tags.bool, tags.null], color: 'var(--warning)' },
  { tag: [tags.comment, tags.meta], color: 'var(--muted-foreground)' },
  { tag: tags.invalid, color: 'var(--destructive)' },
])

/** Controlled document editing. Formatting is explicit and never saves to the API. */
export function CodeEditor({
  id,
  label,
  value,
  onValueChange,
  language = 'text',
  readOnly = false,
  disabled = false,
  className,
  ...aria
}: CodeEditorProps) {
  const host = useRef<HTMLDivElement>(null)
  const view = useRef<EditorView | null>(null)
  const latest = useRef({ value, onValueChange })
  const editable = useRef(new Compartment())
  const attributes = useRef(new Compartment())
  const [preview, setPreview] = useState(false)
  const [formatting, setFormatting] = useState(false)
  const [notice, setNotice] = useState('')

  useEffect(() => {
    latest.current = { value, onValueChange }
  }, [value, onValueChange])
  useEffect(() => {
    if (!host.current) return
    const instance = new EditorView({
      parent: host.current,
      doc: latest.current.value,
      extensions: [
        lineNumbers(),
        highlightActiveLineGutter(),
        drawSelection(),
        history(),
        keymap.of([...defaultKeymap, ...historyKeymap]),
        syntaxHighlighting(highlightStyle),
        editorTheme,
        EditorView.lineWrapping,
        language === 'yaml'
          ? yaml()
          : language === 'json'
            ? json()
            : language === 'shell'
              ? StreamLanguage.define(shell)
              : [],
        editable.current.of([]),
        attributes.current.of([]),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) latest.current.onValueChange?.(update.state.doc.toString())
        }),
      ],
    })
    view.current = instance
    return () => {
      instance.destroy()
      view.current = null
    }
  }, [language])

  useEffect(() => {
    const instance = view.current
    if (!instance) return
    instance.dispatch({
      effects: [
        editable.current.reconfigure([
          EditorState.readOnly.of(readOnly || disabled || preview),
          EditorView.editable.of(!readOnly && !disabled && !preview),
        ]),
        attributes.current.reconfigure(
          EditorView.contentAttributes.of({
            id,
            'aria-label': label,
            'aria-readonly': String(readOnly || disabled || preview),
            ...(aria['aria-describedby'] ? { 'aria-describedby': aria['aria-describedby'] } : {}),
            'aria-invalid': String(aria['aria-invalid'] ?? false),
          }),
        ),
      ],
    })
  }, [id, label, language, readOnly, disabled, preview, aria['aria-describedby'], aria['aria-invalid']])

  useEffect(() => {
    const instance = view.current
    if (instance && value !== instance.state.doc.toString()) {
      // External Controller refreshes update the document without echoing an edit.
      const callback = latest.current.onValueChange
      latest.current.onValueChange = undefined
      instance.dispatch({ changes: { from: 0, to: instance.state.doc.length, insert: value } })
      latest.current.onValueChange = callback
    }
  }, [value, language])

  async function format() {
    if (language !== 'yaml' && language !== 'json') return
    const instance = view.current
    if (!instance) return
    const source = instance.state.doc.toString()
    setFormatting(true)
    setNotice('')
    try {
      const prettier = await import('prettier/standalone')
      const plugins =
        language === 'yaml'
          ? [await import('prettier/plugins/yaml')]
          : [await import('prettier/plugins/babel'), await import('prettier/plugins/estree')]
      const result = await prettier.format(source, {
        parser: language === 'yaml' ? 'yaml' : 'json',
        plugins,
        tabWidth: 2,
      })
      if (view.current === instance && !instance.state.readOnly && instance.state.doc.toString() === source)
        instance.dispatch({ changes: { from: 0, to: source.length, insert: result } })
      else setNotice('Document changed while formatting; your edits were kept.')
    } catch (error) {
      setNotice(error instanceof Error ? error.message : 'Unable to format this document.')
    } finally {
      setFormatting(false)
    }
  }

  return (
    <section
      className={cn('overflow-hidden rounded-xl border border-input bg-card focus-within:border-primary', className)}
      aria-label={label}
    >
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border bg-surface px-3 py-2">
        <span className="text-xs text-muted-foreground">
          {label} <span className="ml-2 font-mono uppercase">{language}</span>
        </span>
        <div className="flex gap-1">
          {!readOnly && (
            <Button
              type="button"
              size="xs"
              variant="ghost"
              disabled={disabled}
              aria-pressed={preview}
              onClick={() => setPreview(!preview)}
            >
              {preview ? 'Edit' : 'Preview'}
            </Button>
          )}
          {!readOnly && (language === 'yaml' || language === 'json') && (
            <Button
              type="button"
              size="xs"
              variant="ghost"
              disabled={disabled || preview || formatting}
              onClick={() => void format()}
            >
              {formatting ? 'Formatting…' : 'Format'}
            </Button>
          )}
          <Button
            type="button"
            size="xs"
            variant="ghost"
            onClick={() => {
              if (!navigator.clipboard) {
                setNotice('Clipboard unavailable. Select the text to copy.')
                return
              }
              void navigator.clipboard.writeText(value).then(
                () => setNotice('Copied.'),
                () => setNotice('Clipboard unavailable. Select the text to copy.'),
              )
            }}
          >
            Copy
          </Button>
        </div>
      </div>
      <div ref={host} className="max-h-[65vh] overflow-auto" />
      <div className="flex items-center justify-between gap-3 border-t border-border px-3 py-2 text-[11px] text-muted-foreground">
        <span role="status" className="whitespace-pre-wrap">
          {notice || (readOnly || preview ? 'Read-only preview' : 'Unsaved document editor')}
        </span>
        <span>{value.split('\n').length} lines</span>
      </div>
    </section>
  )
}
