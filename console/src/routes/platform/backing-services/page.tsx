import { BackingServicesTable } from "@/features/backing-service/backing-services-table";

import { NetworkRange } from "@/components/common/network-range";
import { FormSection } from "@/components/ui/form-section";
import type { BackingHooks } from "@/features/backing-service/api";
import { BackingHookFields } from "@/features/backing-service/hook-fields";

import { Select } from "@/components/ui/select";

import { EmptyState } from "@/components/common/empty-state";
import { PageHeader } from "@/components/common/page-header";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useVisibleServiceObservations } from "@/features/service/use-service-observation-refresh";
import { useStore } from "@/lib/store";
import { useLinkedSlug } from "@/lib/use-linked-slug";
import {
  backingAuthenticationCreateFields,
  type ValkeyAuthenticationSelection,
} from "@/lib/valkey-authentication";
import { Database, Plus } from "lucide-react";
import { useState } from "react";

export default function PlatformBackingServicesPage() {
  const store = useStore();
  const visibleEnvironments = store.backingProjects.flatMap(
    (project) => project.environments?.slice(0, 1) ?? [],
  );
  const observationRefresh = useVisibleServiceObservations({
    environmentIds: visibleEnvironments.map((environment) => environment.id),
    observations: visibleEnvironments.flatMap((environment) =>
      environment.services.map((service) => service.observation),
    ),
    refreshEnvironment: store.refreshEnvironmentServices,
  });
  const [createOpen, setCreateOpen] = useState(false);
  const {
    name,
    slug,
    setName,
    setSlug,
    reset: resetBackingIdentity,
  } = useLinkedSlug();
  const [description, setDescription] = useState("");
  const [adapter, setAdapter] = useState<"postgres:16" | "valkey:9" | "custom">(
    "postgres:16",
  );
  const [image, setImage] = useState("");
  const [hooks, setHooks] = useState<BackingHooks>({});
  const [hookError, setHookError] = useState<string | null>(null);
  const [authentication, setAuthentication] =
    useState<ValkeyAuthenticationSelection>("");
  const [networkPool, setNetworkPool] = useState("10.200.0.0/16");
  const [zoneName, setZoneName] = useState("data");
  const [zoneSubnet, setZoneSubnet] = useState("10.200.20.0/24");
  const [zoneInternal, setZoneInternal] = useState(true);
  const [createError, setCreateError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);

  const openCreate = () => setCreateOpen(true);
  const createFields =
    adapter === "custom"
      ? image.trim()
        ? { adapter: "custom" as const, image: image.trim(), hooks }
        : undefined
      : backingAuthenticationCreateFields(adapter, authentication);

  const createBackingService = async () => {
    if (adapter === "custom" && hookError) return;
    if (!createFields) {
      setCreateError(
        adapter === "custom"
          ? "Enter the container image for this custom backing service."
          : "Select an authentication mode for this Valkey backing service.",
      );
      return;
    }

    setCreating(true);
    setCreateError(null);
    try {
      await store.addBackingProject({
        slug: slug.trim(),
        name: name.trim(),
        description: description.trim() || undefined,
        ...createFields,
        network_pool: networkPool.trim(),
        zone: {
          name: zoneName.trim(),
          subnet: zoneSubnet.trim(),
          internal: zoneInternal,
        },
      });
      setCreateOpen(false);
      resetBackingIdentity();
      setDescription("");
      setImage("");
      setHooks({});
      setHookError(null);
      setAuthentication("");
    } catch (error) {
      setCreateError(
        error instanceof Error
          ? error.message
          : "Unable to create backing service",
      );
    } finally {
      setCreating(false);
    }
  };

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="Backing services"
        description="Shared databases and services for your projects."
        icon={<Database />}
        actions={
          <Button onClick={openCreate}>
            <Plus className="size-4" /> New backing service
          </Button>
        }
      />

      {observationRefresh.refreshError && (
        <p role="alert" className="text-sm text-destructive">
          Runtime refresh failed; evidence will expire locally.{" "}
          {observationRefresh.refreshError}
        </p>
      )}

      {store.backingProjectsLoading ? (
        <EmptyState
          icon={<Database />}
          title="Loading backing services"
          description="Reading the platform-owned backing Project, main Environment, and adapter Service facades."
        />
      ) : store.backingProjectError ? (
        <EmptyState
          icon={<Database />}
          title="Backing services unavailable"
          description={store.backingProjectError}
        />
      ) : store.backingProjects.length === 0 ? (
        <EmptyState
          icon={<Database />}
          title="No backing services"
          description="Create a backing service explicitly — it is never lazily created. Then environments can attach to it."
          action={
            <Button onClick={openCreate}>
              <Plus className="size-4" /> New backing service
            </Button>
          }
        />
      ) : (
        <BackingServicesTable now={observationRefresh.now} />
      )}

      <Drawer open={createOpen} onOpenChange={setCreateOpen}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>New backing service</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-5">
            <FormSection
              title="Identity"
              description="How this backing service appears in Groundplane."
            >
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="backing-name">Name</Label>
                <Input
                  id="backing-name"
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                  placeholder="Primary database"
                  autoFocus
                />
              </div>
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="backing-slug">Slug</Label>
                <Input
                  id="backing-slug"
                  value={slug}
                  onChange={(event) => setSlug(event.target.value)}
                  placeholder="primary-database"
                />
                <p className="text-xs text-muted-foreground">
                  Follows Name until edited.
                </p>
              </div>
              <div className="flex flex-col gap-1.5 sm:col-span-2">
                <Label htmlFor="backing-description">
                  Description (optional)
                </Label>
                <Input
                  id="backing-description"
                  value={description}
                  onChange={(event) => setDescription(event.target.value)}
                  placeholder="Shared application datastore"
                />
              </div>
            </FormSection>
            <FormSection
              title="Service type"
              description="Select a managed engine or run a custom container on its dedicated network."
            >
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="backing-adapter">Adapter</Label>
                <Select
                  id="backing-adapter"
                  value={adapter}
                  onValueChange={(value) => {
                    if (
                      value !== "postgres:16" &&
                      value !== "valkey:9" &&
                      value !== "custom"
                    )
                      return;
                    setAdapter(value);
                    setImage("");
                    setHooks({});
                    setHookError(null);
                    setAuthentication("");
                  }}
                  options={[
                    { value: "postgres:16", label: "PostgreSQL 16" },
                    { value: "valkey:9", label: "Valkey 9" },
                    { value: "custom", label: "Custom container" },
                  ]}
                />
              </div>
              {adapter === "valkey:9" && (
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="backing-authentication">Authentication</Label>
                  <Select
                    id="backing-authentication"
                    value={authentication}
                    placeholder="Select authentication"
                    onValueChange={(value) => {
                      if (
                        value === "username_password" ||
                        value === "password" ||
                        value === "none"
                      )
                        setAuthentication(value);
                    }}
                    options={[
                      {
                        value: "username_password",
                        label: "Username + password",
                      },
                      { value: "password", label: "Password only" },
                      { value: "none", label: "None · no authentication" },
                    ]}
                  />
                  <p className="text-xs text-muted-foreground">
                    Immutable for this backing instance. Every Attach inherits
                    this mode.
                  </p>
                </div>
              )}
              {adapter === "custom" && (
                <div className="flex flex-col gap-1.5 sm:col-span-2">
                  <Label htmlFor="backing-image">Container image</Label>
                  <Input
                    id="backing-image"
                    value={image}
                    onChange={(event) => setImage(event.target.value)}
                    placeholder="registry.example/internal/search:1.4"
                  />
                  <p className="text-xs text-muted-foreground">
                    Groundplane runs your image and connects consumers to its
                    network. Optional hooks can provision credentials and facts.
                    No volumes, health checks, grants or backups are added
                    automatically.
                  </p>
                </div>
              )}
            </FormSection>
            <FormSection
              title="Network"
              description="Reserve a pool for this backing Environment, then choose its zone subnet within that pool."
            >
              <div className="flex flex-col gap-1.5 sm:col-span-2">
                <Label htmlFor="backing-pool">Environment pool (CIDR)</Label>
                <Input
                  id="backing-pool"
                  value={networkPool}
                  onChange={(event) => setNetworkPool(event.target.value)}
                  placeholder="10.200.0.0/16"
                />
              </div>
              <NetworkRange cidr={networkPool} label="Environment pool" />
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="backing-zone-name">Zone name</Label>
                <Input
                  id="backing-zone-name"
                  value={zoneName}
                  onChange={(event) => setZoneName(event.target.value)}
                  placeholder="data"
                />
              </div>
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="backing-subnet">Zone subnet (CIDR)</Label>
                <Input
                  id="backing-subnet"
                  value={zoneSubnet}
                  onChange={(event) => setZoneSubnet(event.target.value)}
                  placeholder="10.200.20.0/24"
                />
              </div>
              <label className="flex items-center gap-2 text-sm sm:col-span-2">
                <Checkbox
                  checked={zoneInternal}
                  onChange={(event) => setZoneInternal(event.target.checked)}
                />
                Internal network (no external access through this zone)
              </label>
              <p className="text-xs text-muted-foreground sm:col-span-2">
                Services on this zone can communicate with each other. External
                access requires another, non-internal zone. This does not change
                the host&apos;s internet access.
              </p>
            </FormSection>
            {adapter === "custom" && (
              <BackingHookFields
                value={hooks}
                onChange={setHooks}
                onError={setHookError}
              />
            )}
            {adapter === "custom" && hookError && (
              <p role="alert" className="text-xs text-destructive">
                {hookError}
              </p>
            )}
            {adapter === "valkey:9" && authentication === "none" && (
              <p
                role="status"
                className="rounded-lg border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning sm:col-span-2"
              >
                Any client that can reach this backing service can access it
                without authentication.
              </p>
            )}
            {createError && (
              <p
                role="alert"
                className="text-xs text-destructive sm:col-span-2"
              >
                {createError}
              </p>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setCreateOpen(false)}>
              Cancel
            </Button>
            <Button
              disabled={
                creating ||
                (adapter === "custom" && !!hookError) ||
                !createFields ||
                !slug.trim() ||
                !name.trim() ||
                !networkPool.trim() ||
                !zoneName.trim() ||
                !zoneSubnet.trim()
              }
              onClick={() => void createBackingService()}
            >
              {creating ? "Creating..." : "Create backing service"}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
    </div>
  );
}
