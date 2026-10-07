import { PageHeader } from "@/components/common/page-header";
import { SummaryStrip, SummaryItem } from "@/components/common/resource-panel";
import { HelpHint } from "@/components/common/resource-panel";
import { ResourceRow, ResourceTable } from "@/components/common/resource-table";
import {
  CollectionToolbar,
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select } from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ArrowLeft, Boxes, RefreshCw, Trash2 } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { imageReferences, imageSize, type HostImage } from "./api";
import { ImageDetails } from "./image-details";
import { ImageFetchAction } from "./image-fetch-action";
import {
  imageName,
  imageRepositories,
  imageTag,
  shortImageId,
} from "./image-presentation";
import { RemoveImageDialog } from "./remove-image-dialog";
import { useImageInventory } from "./use-image-inventory";

export default function ImagesPage() {
  const { inventory, loading, error, refresh } = useImageInventory();
  const [params, setParams] = useSearchParams();
  const search = params.get("q") ?? "";
  const usage = params.get("usage") ?? "all";
  const selected = params.get("image");
  function setFilter(key: string, value: string) {
    setParams(
      (current) => {
        const next = new URLSearchParams(current);
        if (value) next.set(key, value);
        else next.delete(key);
        return next;
      },
      { replace: true },
    );
  }
  const setSearch = (value: string) => setFilter("q", value);
  const setUsage = (value: string) => setFilter("usage", value);
  function setSelected(id: string | null, tab = "overview") {
    setParams((current) => {
      const next = new URLSearchParams(current);
      next.delete("imageTab");
      if (id) {
        next.set("image", id);
        next.set("imageTab", tab);
      } else next.delete("image");
      return next;
    });
  }
  const [removing, setRemoving] = useState<HostImage | null>(null);
  useEffect(() => {
    const timer = setInterval(() => {
      if (!document.hidden) void refresh();
    }, 10000);
    return () => clearInterval(timer);
  }, [refresh]);
  const images = useMemo(
    () =>
      (inventory?.images ?? [])
        .filter(
          (image) =>
            (usage === "all" ||
              (usage === "containers"
                ? image.containers > 0
                : usage === "removable"
                  ? !image.removal_blocked
                  : !!image.removal_blocked)) &&
            [
              image.id,
              ...imageReferences(image),
              ...image.fetches.map((fetch) => fetch.requested),
            ].some((value) =>
              value.toLowerCase().includes(search.toLowerCase()),
            ),
        )
        .sort(
          (a, b) =>
            imageName(a).localeCompare(imageName(b)) ||
            b.created_at.localeCompare(a.created_at),
        ),
    [inventory, search, usage],
  );
  const detail = inventory?.images.find((image) => image.id === selected);
  const table = useTableView(
    images,
    {
      name: imageName,
      created: (image) => Date.parse(image.created_at),
      size: (image) => image.size_bytes,
    },
    "name",
    "asc",
    `${search}/${usage}`,
  );

  return (
    <div className="flex min-w-0 flex-col gap-5">
      {!selected && (
        <>
          <Link
            to="/platform/host"
            className="flex w-fit items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground"
          >
            <ArrowLeft className="size-3.5" /> Host
          </Link>
          <PageHeader
            title="Images"
            description="Host Agent inventory, with every local tag grouped under its immutable image."
            icon={<Boxes />}
            actions={
              <>
                <Button
                  variant="outline"
                  size="icon"
                  aria-label="Refresh images"
                  onClick={() => void refresh()}
                  disabled={loading}
                >
                  <RefreshCw className="size-4" />
                </Button>
                <ImageFetchAction onSettled={refresh} />
              </>
            }
          />
          {inventory && (
            <SummaryStrip>
              <SummaryItem label="Local images">
                {inventory.images.length}
              </SummaryItem>
              <SummaryItem label="In use by containers">
                {inventory.images.filter((i) => i.containers > 0).length}
              </SummaryItem>
              <SummaryItem label="No containers">
                {inventory.images.filter((i) => i.containers === 0).length}
              </SummaryItem>
              <SummaryItem label="Stored size">
                {imageSize(
                  inventory.images.reduce(
                    (total, image) => total + image.size_bytes,
                    0,
                  ),
                )}
              </SummaryItem>
            </SummaryStrip>
          )}
          <Card>
            <CardHeader>
              <CardTitle>
                <h2>Image inventory</h2>
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-4 p-4 sm:p-5">
              <CollectionToolbar
                query={search}
                onQueryChange={setSearch}
                label="images"
              >
                <Select
                  aria-label="Image usage"
                  value={usage}
                  onValueChange={setUsage}
                  className="w-full sm:w-44"
                  options={[
                    { value: "all", label: "All images" },
                    { value: "containers", label: "Used by containers" },
                    { value: "removable", label: "Removable" },
                    { value: "protected", label: "Protected" },
                  ]}
                />
                <div className="flex w-full items-center gap-2 md:hidden">
                  <Select
                    aria-label="Sort images by"
                    value={table.field}
                    onValueChange={table.sortBy}
                    className="flex-1"
                    options={[
                      { value: "name", label: "Image name" },
                      { value: "created", label: "Creation date" },
                      { value: "size", label: "Size" },
                    ]}
                  />
                  <Button
                    variant="outline"
                    onClick={() => table.sortBy(table.field)}
                    aria-label="Reverse image sort"
                  >
                    {table.direction === "asc" ? "Ascending ↑" : "Descending ↓"}
                  </Button>
                </div>
              </CollectionToolbar>
              {error && (
                <p role="alert" className="text-sm text-destructive">
                  {error} {inventory ? "The list may be stale." : ""}
                </p>
              )}
              {!inventory && loading && (
                <p role="status" className="text-sm text-muted-foreground">
                  Loading host images…
                </p>
              )}
              {inventory && (
                <>
                  <div className="flex flex-wrap justify-between gap-2 text-xs text-muted-foreground">
                    <span>
                      {images.length} of {inventory.images.length} images
                    </span>
                    <span>
                      Updated{" "}
                      {new Date(inventory.observed_at).toLocaleTimeString()}
                    </span>
                  </div>
                  <ResourceTable>
                    <Table className="table-fixed">
                      <TableHeader>
                        <TableRow>
                          <TableSortHead sort={table} field="name">
                            Image
                          </TableSortHead>
                          <TableHead className="hidden w-[18%] lg:table-cell">
                            Tags
                          </TableHead>
                          <TableSortHead
                            sort={table}
                            field="created"
                            className="hidden w-32 md:table-cell"
                          >
                            Created
                          </TableSortHead>
                          <TableSortHead
                            sort={table}
                            field="size"
                            className="hidden w-24 sm:table-cell"
                          >
                            Size
                          </TableSortHead>
                          <TableHead className="w-28 sm:w-36">Usage</TableHead>
                          <TableHead className="w-12">
                            <span className="sr-only">Actions</span>
                          </TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {table.rows.map((image) => {
                          const repositories = imageRepositories(image);
                          const tags = [...new Set(image.tags.map(imageTag))];
                          return (
                            <ResourceRow
                              key={image.id}
                              onOpen={() => setSelected(image.id)}
                            >
                              <TableCell>
                                <Button
                                  variant="ghost"
                                  size="content"
                                  className="flex w-full min-w-0 flex-col items-start gap-1 text-left"
                                  onClick={() => setSelected(image.id)}
                                  aria-label={`Inspect ${imageName(image)}, image ${shortImageId(image.id)}`}
                                >
                                  <span className="w-full truncate text-sm font-medium">
                                    {imageName(image)}
                                  </span>
                                  <span className="w-full truncate text-xs font-normal text-muted-foreground">
                                    {repositories[0] ?? "No repository"}
                                    {repositories.length > 1
                                      ? ` +${repositories.length - 1}`
                                      : ""}
                                  </span>
                                  <code className="text-xs font-normal text-muted-foreground/70">
                                    {shortImageId(image.id)}
                                  </code>
                                </Button>
                              </TableCell>
                              <TableCell className="hidden lg:table-cell">
                                <div className="flex flex-wrap gap-1">
                                  {tags.slice(0, 2).map((tag) => (
                                    <Badge
                                      key={tag}
                                      variant="outline"
                                      className="max-w-full"
                                      title={tag}
                                    >
                                      <span className="truncate">{tag}</span>
                                    </Badge>
                                  ))}
                                  {tags.length > 2 && (
                                    <Badge variant="muted">
                                      +{tags.length - 2} tags
                                    </Badge>
                                  )}
                                  {!tags.length && (
                                    <span className="text-xs text-muted-foreground">
                                      By digest
                                    </span>
                                  )}
                                </div>
                              </TableCell>
                              <TableCell
                                className="hidden text-xs text-muted-foreground md:table-cell"
                                title={new Date(
                                  image.created_at,
                                ).toLocaleString()}
                              >
                                {new Date(
                                  image.created_at,
                                ).toLocaleDateString()}
                              </TableCell>
                              <TableCell className="hidden whitespace-nowrap text-xs sm:table-cell">
                                {imageSize(image.size_bytes)}
                              </TableCell>
                              <TableCell>
                                <Button
                                  variant="ghost"
                                  size="content"
                                  aria-label={`Show usage of ${imageName(image)}`}
                                  onClick={(event) => {
                                    event.stopPropagation();
                                    setSelected(image.id, "usage");
                                  }}
                                >
                                  <Badge
                                    variant={
                                      image.containers > 0
                                        ? "primary"
                                        : image.removal_blocked
                                          ? "muted"
                                          : "success"
                                    }
                                    title={
                                      image.removal_blocked ||
                                      "No container or retained runtime requires this image"
                                    }
                                  >
                                    {image.containers > 0
                                      ? `${image.containers} containers`
                                      : image.removal_blocked
                                        ? "Protected"
                                        : "Unused"}
                                  </Badge>
                                </Button>
                              </TableCell>
                              <TableCell className="px-1">
                                <Button
                                  variant="ghost"
                                  size="icon"
                                  aria-label={`Remove ${imageName(image)}, image ${shortImageId(image.id)}`}
                                  title={
                                    image.removal_blocked ||
                                    "Remove image and its tags"
                                  }
                                  disabled={
                                    !!image.removal_blocked ||
                                    !!error ||
                                    loading
                                  }
                                  onClick={(event) => {
                                    event.stopPropagation();
                                    setRemoving(image);
                                  }}
                                >
                                  <Trash2 className="size-4" />
                                </Button>
                              </TableCell>
                            </ResourceRow>
                          );
                        })}
                      </TableBody>
                    </Table>
                  </ResourceTable>
                  <TablePagination table={table} label="Images" />
                  {images.length === 0 && (
                    <p className="py-6 text-center text-sm text-muted-foreground">
                      {inventory.images.length === 0
                        ? "No images yet. Fetch an image to get started."
                        : "No images match these filters."}
                    </p>
                  )}
                  <div className="flex items-center gap-2 text-xs text-muted-foreground">
                    <HelpHint label="About image usage">
                      One row per image, including all its tags. Container
                      counts include stopped containers. Open a row for usage,
                      tag history and removal protection.
                    </HelpHint>
                    Image usage
                  </div>
                </>
              )}
            </CardContent>
          </Card>
        </>
      )}
      {selected && !detail && (
        <div className="space-y-4">
          <Button variant="outline" onClick={() => setSelected(null)}>
            <ArrowLeft className="size-4" /> All images
          </Button>
          <p role="status" className="text-sm text-muted-foreground">
            {loading
              ? "Loading image…"
              : error || "This image is no longer in the host inventory."}
          </p>
        </div>
      )}
      {detail && (
        <ImageDetails
          image={detail}
          observedAt={inventory?.observed_at}
          unavailable={!!error || loading}
          onClose={() => setSelected(null)}
          onRemove={() => {
            setRemoving(detail);
          }}
        />
      )}
      {removing && (
        <RemoveImageDialog
          image={removing}
          onClose={() => setRemoving(null)}
          onSettled={refresh}
        />
      )}
    </div>
  );
}
