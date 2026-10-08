import { useState } from "react";
import { ArrowUpCircle } from "lucide-react";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { ImagePicker } from "@/features/image-delivery/image-picker";
import { listImages } from "@/features/image-delivery/api";
import { managedImageReferences, managedUpdateReference } from "./managed-images";
import { useAdapterCatalog } from "./use-adapter-catalog";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";

// Save uses the ordinary desired-state edit. Deploy uses the same protected
// execution path as other Services, not a database-specific host updater.
export function ManagedImageUpdate({ env, service }: { env: Environment; service: Service }) {
  const store = useStore();
  const [open, setOpen] = useState(false);
  const [image, setImage] = useState(service.image);
  const catalog = useAdapterCatalog(open);
  const adapter = catalog.catalog?.items?.find(item => item.key === service.adapter);
  const version = adapter?.versions?.find(item => item.version === service.adapterVersion);
  const imagePattern = version ? new RegExp(version.image_pattern) : null;
  return <>
    <Button variant="outline" onClick={() => { setImage(service.image); setOpen(true); }}>
      <ArrowUpCircle className="size-4" /> Update image
    </Button>
    {open && <TaskRunnerDialog open onOpenChange={setOpen} variant="drawer"
      title={`Update ${adapter?.label ?? service.adapter} image`} type="deploy" target={service.name} workspace="platform"
      description="Replace the database container within its selected server version. Connections are interrupted during restart; data and backup tools are retained. Major-version changes are not supported."
      startLabel="Save and deploy" startDisabled={!image.trim() || !imagePattern} steps={[]}
      review={<div className="space-y-3">
        <Label htmlFor="managed-update-image">Upstream image · {service.adapterVersion}</Label>
        <ImagePicker id="managed-update-image" value={image} onChange={setImage} referencesForImage={item => imagePattern ? managedImageReferences(item, imagePattern) : []} />
        {catalog.error && <p role="alert" className="text-sm text-destructive">{catalog.error} <Button variant="outline" size="sm" onClick={catalog.retry}>Retry</Button></p>}
        <p className="text-sm text-muted-foreground">Fetch the image on Host → Images first. If Deploy fails, the image choice remains saved. Inspect the Task before retrying or selecting another supported patch.</p>
      </div>}
      onDispatch={async () => {
        if (!imagePattern) throw new Error('Supported server versions are unavailable.')
        const selected = managedUpdateReference(image.trim(), (await listImages()).images, imagePattern);
        await store.updateService(env.id, service.id, { ...service, image: selected, strategy: "recreate", onFailure: "leave_active" });
        return store.commitDeploy(env.id, service.name, selected, "recreate", "leave_active");
      }}
      onSettled={async () => { await Promise.all([store.refreshEnvironmentServices(env.id), store.refreshEnvironmentReleases(env.id)]); }}
    />}
  </>;
}
