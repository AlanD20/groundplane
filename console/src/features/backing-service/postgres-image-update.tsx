import { useState } from "react";
import { ArrowUpCircle } from "lucide-react";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { ImagePicker } from "@/features/image-delivery/image-picker";
import { listImages } from "@/features/image-delivery/api";
import { postgresImageReferences, postgresUpdateReference } from "./postgres-images";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";

// Save uses the ordinary desired-state edit. Deploy uses the same protected
// execution path as other Services, not a database-specific host updater.
export function PostgresImageUpdate({ env, service }: { env: Environment; service: Service }) {
  const store = useStore();
  const [open, setOpen] = useState(false);
  const [image, setImage] = useState(service.image);
  return <>
    <Button variant="outline" onClick={() => { setImage(service.image); setOpen(true); }}>
      <ArrowUpCircle className="size-4" /> Update image
    </Button>
    {open && <TaskRunnerDialog open onOpenChange={setOpen} variant="drawer"
      title="Update PostgreSQL image" type="deploy" target={service.name} workspace="platform"
      description="Replace the database container with an upstream PostgreSQL 16 Alpine image. Connections are interrupted during restart; the data Volume and backup tools are retained. Major-version changes are not supported."
      startLabel="Save and deploy" startDisabled={!image.trim()} steps={[]}
      review={<div className="space-y-3">
        <Label htmlFor="postgres-update-image">Upstream image</Label>
        <ImagePicker id="postgres-update-image" value={image} onChange={setImage} referencesForImage={postgresImageReferences} />
        <p className="text-sm text-muted-foreground">Fetch the image on Host → Images first. If Deploy fails, the image choice remains saved and the candidate stays in place. Inspect the Task before retrying or selecting another PostgreSQL 16 image.</p>
      </div>}
      onDispatch={async () => {
        const selected = postgresUpdateReference(image.trim(), (await listImages()).images);
        await store.updateService(env.id, service.id, { ...service, image: selected, strategy: "recreate", onFailure: "leave_active" });
        return store.commitDeploy(env.id, service.name, selected, "recreate", "leave_active");
      }}
      onSettled={async () => { await Promise.all([store.refreshEnvironmentServices(env.id), store.refreshEnvironmentReleases(env.id)]); }}
    />}
  </>;
}
