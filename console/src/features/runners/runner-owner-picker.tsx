import { Label } from "@/components/ui/label";
import { SearchableSelect } from "@/components/ui/searchable-select";

export function RunnerOwnerPicker({
  scope,
  tenant,
  projects,
  environments,
  value,
  onChange,
}: {
  scope?: { kind: "Project" | "Environment"; id: string; name: string };
  tenant: string;
  projects: { id: string; slug: string }[];
  environments: { id: string; label: string }[];
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <div className="space-y-2">
      <Label htmlFor="runner-create-owner">Owner</Label>
      <SearchableSelect
        id="runner-create-owner"
        value={value}
        onValueChange={onChange}
        disabled={!!scope}
        options={
          scope
            ? [{ value: scope.id, label: `${scope.kind} · ${scope.name}` }]
            : [
                { value: "tenant", label: `Tenant · ${tenant}` },
                ...projects.map((project) => ({
                  value: project.id,
                  label: `Project · ${project.slug}`,
                })),
                ...environments.map((environment) => ({
                  value: environment.id,
                  label: `Environment · ${environment.label}`,
                })),
              ]
        }
      />
      <p className="text-xs text-muted-foreground">
        Owner is permanent. Trusted workflows use normal GP CLI/API access.
      </p>
    </div>
  );
}
