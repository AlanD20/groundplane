import { useEditorSession } from "@/components/common/editor-session";
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useStore } from "@/lib/store";
import { useLinkedSlug } from "@/lib/use-linked-slug";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  ResourceForm,
  ResourceFormHeader as DialogHeader,
  ResourceFormTitle as DialogTitle,
  ResourceFormFooter as DialogFooter,
} from "@/components/common/resource-form";

export function TenantCreateForm({ onClose }: { onClose: () => void }) {
  const editorSession = useEditorSession();
  const { addTenant } = useStore();
  const navigate = useNavigate();
  const setNewTenantOpen = (open: boolean) => {
    if (!open) onClose();
  };

  const {
    name,
    slug,
    setName,
    setSlug,
    reset: resetTenantIdentity,
  } = useLinkedSlug();
  const [description, setDescription] = useState("");
  const [createError, setCreateError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);

  async function createTenant() {
    const s = slug.trim();
    if (creating || !s) return;
    setCreating(true);
    setCreateError(null);
    try {
      const tenant = await addTenant({
        slug: s,
        name: name.trim() || s,
        description: description.trim(),
      });
      editorSession?.finish();
      setNewTenantOpen(false);
      resetTenantIdentity();
      setDescription("");
      navigate(`/t/${tenant.slug}`);
    } catch (error) {
      setCreateError(
        error instanceof Error ? error.message : "Unable to create tenant",
      );
    } finally {
      setCreating(false);
    }
  }

  return (
    <ResourceForm open onOpenChange={setNewTenantOpen}>
      <form
        className="flex flex-col gap-5"
        onSubmit={(event) => {
          event.preventDefault();
          if (name.trim() && slug.trim()) void createTenant();
        }}
      >
        <DialogHeader>
          <DialogTitle>New tenant</DialogTitle>
        </DialogHeader>
        <div className="grid max-w-5xl items-start gap-4 md:grid-cols-2">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="t-name">Display name</Label>
            <Input
              id="t-name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Acme Inc."
              autoFocus
            />
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="t-slug">Slug</Label>
            <Input
              id="t-slug"
              value={slug}
              onChange={(e) => setSlug(e.target.value)}
              placeholder="acme"
            />
          </div>
          <div className="flex flex-col gap-1.5 md:col-span-2">
            <Label htmlFor="t-desc">Description</Label>
            <Input
              id="t-desc"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="What this tenant hosts"
            />
          </div>
          {createError && (
            <p role="alert" className="text-xs text-destructive md:col-span-2">
              {createError}
            </p>
          )}
        </div>
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => setNewTenantOpen(false)}
          >
            Cancel
          </Button>
          <Button
            type="submit"
            disabled={creating || !name.trim() || !slug.trim()}
          >
            {creating ? "Creating…" : "Create tenant"}
          </Button>
        </DialogFooter>
      </form>
    </ResourceForm>
  );
}
