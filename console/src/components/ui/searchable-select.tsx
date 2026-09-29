import { useRef } from 'react'
import { Combobox } from '@base-ui/react/combobox'
import { Check, ChevronDown, Search } from 'lucide-react'
import { cn } from '@/lib/utils'
import type { SelectOption } from './select'

export function SearchableSelect({ value, onValueChange, options, placeholder = 'Select…', searchPlaceholder = 'Search…', emptyText = 'No matches.', disabled, className, 'aria-label': ariaLabel }: {
  value: string | null
  onValueChange: (value: string) => void
  options: SelectOption[]
  placeholder?: string
  searchPlaceholder?: string
  emptyText?: string
  disabled?: boolean
  className?: string
  'aria-label': string
}) {
  const input = useRef<HTMLInputElement>(null)
  return <Combobox.Root items={options} value={options.find(option => option.value === value) ?? null}
    isItemEqualToValue={(item, selected) => item.value === selected.value}
    onValueChange={option => { if (option) onValueChange(option.value) }} disabled={disabled}>
    <Combobox.Trigger aria-label={ariaLabel} className={cn('flex h-10 w-full min-w-0 items-center justify-between gap-2 rounded-lg border border-input bg-background px-3 text-sm outline-none transition-colors hover:bg-muted/50 focus-visible:border-ring focus-visible:ring-1 focus-visible:ring-ring/70 disabled:opacity-50', className)}>
      <span className="min-w-0 truncate"><Combobox.Value placeholder={placeholder} /></span><ChevronDown className="size-3.5 shrink-0 text-muted-foreground" />
    </Combobox.Trigger>
    <Combobox.Portal><Combobox.Positioner sideOffset={6} className="z-50 outline-none">
      <Combobox.Popup initialFocus={input} className="w-[var(--anchor-width)] max-w-[calc(100vw-2rem)] overflow-hidden rounded-lg border border-border bg-popover p-1 text-popover-foreground shadow-xl outline-none transition-opacity duration-150 data-[ending-style]:opacity-0 data-[starting-style]:opacity-0">
        <div className="flex items-center gap-2 border-b border-border px-2"><Search className="size-4 shrink-0 text-muted-foreground" /><Combobox.Input ref={input} aria-label={searchPlaceholder} placeholder={searchPlaceholder} className="h-10 min-w-0 flex-1 bg-transparent text-sm outline-none" /></div>
        <Combobox.Empty className="p-3 text-sm text-muted-foreground">{emptyText}</Combobox.Empty>
        <Combobox.List className="max-h-64 overflow-y-auto overscroll-contain">
          {(option: SelectOption) => <Combobox.Item key={option.value} value={option} className="flex cursor-default items-start justify-between gap-2 rounded-md p-2 text-sm outline-none select-none data-[selected]:bg-accent data-[selected]:text-primary data-[highlighted]:bg-muted">
            <span className="min-w-0 break-all">{option.label}</span><Combobox.ItemIndicator><Check className="mt-0.5 size-3.5 shrink-0 text-primary" /></Combobox.ItemIndicator>
          </Combobox.Item>}
        </Combobox.List>
      </Combobox.Popup>
    </Combobox.Positioner></Combobox.Portal>
  </Combobox.Root>
}
