import { Label } from '@/components/ui/label'
import { Select } from '@/components/ui/select'
import type { Environment } from '@/lib/types'

export function BlueprintApplyScope({ environment, value, onChange, disabled = false }: {
  environment: Environment
  value: string
  onChange: (value: string) => void
  disabled?: boolean
}) {
  return (
    <div className="space-y-2">
      <Label htmlFor={`blueprint-scope-${environment.id}`}>Apply scope</Label>
      <Select
        id={`blueprint-scope-${environment.id}`}
        value={value || '__environment__'}
        disabled={disabled}
        onValueChange={(next) => onChange(next === '__environment__' ? '' : next)}
        options={[
          { value: '__environment__', label: 'Entire Environment' },
          ...environment.services.map((service) => ({ value: service.name, label: service.name })),
        ]}
      />
      <p className="text-xs text-muted-foreground">
        {value ? 'Only this Service is applied. Shared changes require full Apply; dependencies are not deployed.' : 'Apply the complete Environment Blueprint.'}
        {' '}Running workloads follow their release strategy; recreate causes downtime.
      </p>
    </div>
  )
}
