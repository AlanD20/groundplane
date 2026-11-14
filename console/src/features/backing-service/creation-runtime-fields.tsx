import { FormSection } from "@/components/ui/form-section";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export function BackingCreationRuntimeFields(props: {
  custom: boolean;
  memory: string;
  cpus: string;
  volumeSlug: string;
  mountPath: string;
  onMemoryChange: (value: string) => void;
  onCpusChange: (value: string) => void;
  onVolumeSlugChange: (value: string) => void;
  onMountPathChange: (value: string) => void;
}) {
  return (
    <>
      <FormSection
        title="Resources"
        description="Optional limits apply from the first container start."
      >
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="backing-memory">Memory limit</Label>
          <Input
            id="backing-memory"
            value={props.memory}
            onChange={(event) => props.onMemoryChange(event.target.value)}
            placeholder="1536m"
          />
        </div>
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="backing-cpus">CPU limit</Label>
          <Input
            id="backing-cpus"
            type="number"
            min="0"
            step="0.1"
            value={props.cpus}
            onChange={(event) => props.onCpusChange(event.target.value)}
            placeholder="1"
          />
        </div>
      </FormSection>
      {props.custom && (
        <FormSection
          title="Persistent storage"
          description="Optional managed Volume. Leaving both fields empty creates no Volume."
        >
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="backing-volume">Volume slug</Label>
            <Input
              id="backing-volume"
              value={props.volumeSlug}
              onChange={(event) => props.onVolumeSlugChange(event.target.value)}
              placeholder="scanner-signatures"
            />
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="backing-mount">Container mount path</Label>
            <Input
              id="backing-mount"
              value={props.mountPath}
              onChange={(event) => props.onMountPathChange(event.target.value)}
              placeholder="/var/lib/clamav"
            />
          </div>
        </FormSection>
      )}
    </>
  );
}
