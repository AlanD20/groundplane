import { useEffect, useRef, useState, type ReactNode } from "react";
import { Maximize, Minimize, Minus, Plus, Scan } from "lucide-react";
import { useWorkspaceFullscreen } from "./use-workspace-fullscreen";
import { Button } from "@/components/ui/button";

/** Shared pan/zoom viewport; transforms presentation without changing node state. */
export function TopologyCanvas({
  width,
  height,
  children,
  controls,
}: {
  width: number;
  height: number;
  children: ReactNode;
  controls?: ReactNode;
}) {
  const board = useRef<HTMLDivElement>(null);
  const [fullscreen, setFullscreen] = useWorkspaceFullscreen(undefined, () =>
    board.current?.focus(),
  );
  const [camera, setCamera] = useState({ x: 16, y: 16, zoom: 0.75 });
  const cameraRef = useRef(camera);
  const [dragging, setDragging] = useState(false);
  const drag = useRef<{
    id: number;
    x: number;
    y: number;
    originX: number;
    originY: number;
  } | null>(null);
  const zoom = camera.zoom;
  const moveCamera = (next: typeof camera) => {
    cameraRef.current = next;
    setCamera(next);
  };
  const zoomAt = (scale: number, x?: number, y?: number) => {
    const element = board.current;
    if (!element) return;
    const previous = cameraRef.current;
    const next = Math.max(0.15, Math.min(2.5, scale));
    const px = x ?? element.clientWidth / 2,
      py = y ?? element.clientHeight / 2;
    moveCamera({
      zoom: next,
      x: px - ((px - previous.x) * next) / previous.zoom,
      y: py - ((py - previous.y) * next) / previous.zoom,
    });
  };
  const setZoom = (scale: number) => zoomAt(scale);
  useEffect(() => {
    const element = board.current;
    if (!element) return;
    const wheel = (event: WheelEvent) => {
      if ((event.target as Element).closest("[data-board-controls]")) return;
      event.preventDefault();
      const unit =
        event.deltaMode === 1
          ? 16
          : event.deltaMode === 2
            ? element.clientHeight
            : 1;
      if (event.ctrlKey || event.metaKey) {
        const bounds = element.getBoundingClientRect();
        zoomAt(
          cameraRef.current.zoom * Math.exp(-event.deltaY * unit * 0.003),
          event.clientX - bounds.left,
          event.clientY - bounds.top,
        );
      } else {
        const previous = cameraRef.current;
        moveCamera({
          ...previous,
          x: previous.x - event.deltaX * unit,
          y: previous.y - event.deltaY * unit,
        });
      }
    };
    element.addEventListener("wheel", wheel, { passive: false });
    return () => element.removeEventListener("wheel", wheel);
  }, []);

  return (
    <div
      className={
        fullscreen
          ? "workspace-fullscreen flex flex-col overflow-hidden bg-card"
          : "flex h-[65vh] flex-col overflow-hidden rounded-xl border border-border bg-card"
      }
    >
      <div
        data-board-controls
        className="relative z-10 flex shrink-0 flex-wrap items-center gap-2 border-b border-border bg-card p-2"
      >
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            const element = board.current;
            if (element)
              moveCamera({
                x: 16,
                y: 16,
                zoom: Math.max(
                  0.15,
                  Math.min(1, (element.clientWidth - 32) / width),
                ),
              });
          }}
        >
          <Scan /> Fit width
        </Button>
        <Button
          variant="outline"
          size="sm"
          aria-label="Zoom out"
          onClick={() => setZoom(zoom / 1.2)}
        >
          <Minus />
        </Button>
        <Button
          variant="ghost"
          size="sm"
          aria-label="Reset zoom to 100%"
          onClick={() => setZoom(1)}
        >
          {Math.round(zoom * 100)}%
        </Button>
        <Button
          variant="outline"
          size="sm"
          aria-label="Zoom in"
          onClick={() => setZoom(zoom * 1.2)}
        >
          <Plus />
        </Button>
        <Button
          variant="outline"
          size="sm"
          aria-label={fullscreen ? "Exit fullscreen board" : "Fullscreen board"}
          aria-pressed={fullscreen}
          onClick={() => setFullscreen(!fullscreen)}
        >
          {fullscreen ? <Minimize /> : <Maximize />}
        </Button>
        {controls}
      </div>
      <div
        className="relative min-h-0 flex-1"
        ref={board}
        data-graph-canvas
        tabIndex={0}
        role="region"
        aria-label="Topology board"
        aria-keyshortcuts="Control++ Control+- Control+0 Meta++ Meta+- Meta+0"
        title="Drag empty space to pan. Ctrl/Cmd + wheel or +/− to zoom. Ctrl/Cmd + 0 to reset."
        onKeyDown={(event) => {
          if (
            (event.target as Element).closest(
              "input,textarea,[contenteditable=true]",
            )
          )
            return;
          if (event.ctrlKey || event.metaKey) {
            if (["+", "=", "-", "0"].includes(event.key)) {
              event.preventDefault();
              event.stopPropagation();
              if (event.key === "0") moveCamera({ x: 16, y: 16, zoom: 0.75 });
              else
                zoomAt(
                  cameraRef.current.zoom * (event.key === "-" ? 1 / 1.2 : 1.2),
                );
            }
          } else if (
            ["ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown"].includes(
              event.key,
            )
          ) {
            event.preventDefault();
            event.stopPropagation();
            const c = cameraRef.current;
            moveCamera({
              ...c,
              x:
                c.x +
                (event.key === "ArrowLeft"
                  ? 60
                  : event.key === "ArrowRight"
                    ? -60
                    : 0),
              y:
                c.y +
                (event.key === "ArrowUp"
                  ? 60
                  : event.key === "ArrowDown"
                    ? -60
                    : 0),
            });
          }
        }}
        onPointerDown={(event) => {
          if (event.button !== 0 || (event.target as Element).closest("button"))
            return;
          event.preventDefault();
          event.currentTarget.focus({ preventScroll: true });
          event.currentTarget.setPointerCapture(event.pointerId);
          drag.current = {
            id: event.pointerId,
            x: event.clientX,
            y: event.clientY,
            originX: cameraRef.current.x,
            originY: cameraRef.current.y,
          };
          setDragging(true);
        }}
        onPointerMove={(event) => {
          const start = drag.current;
          if (!start || start.id !== event.pointerId) return;
          moveCamera({
            ...cameraRef.current,
            x: start.originX + event.clientX - start.x,
            y: start.originY + event.clientY - start.y,
          });
        }}
        onPointerUp={(event) => {
          if (drag.current?.id === event.pointerId) {
            drag.current = null;
            setDragging(false);
            event.currentTarget.releasePointerCapture(event.pointerId);
          }
        }}
        onPointerCancel={() => {
          drag.current = null;
          setDragging(false);
        }}
        onLostPointerCapture={() => {
          drag.current = null;
          setDragging(false);
        }}
        style={{
          position: "relative",
          overflow: "hidden",
          touchAction: "none",
          userSelect: "none",
          cursor: dragging ? "grabbing" : "grab",
          background: "var(--card)",
          backgroundImage:
            "radial-gradient(var(--border) 1px, transparent 1px)",
          backgroundSize: `${20 * zoom}px ${20 * zoom}px`,
          backgroundPosition: `${camera.x}px ${camera.y}px`,
        }}
      >
        <svg
          width={width}
          height={height}
          viewBox={`0 0 ${width} ${height}`}
          style={{
            position: "absolute",
            left: 0,
            top: 0,
            transformOrigin: "0 0",
            transform: `translate(${camera.x}px,${camera.y}px) scale(${zoom})`,
          }}
          role="group"
          aria-label="Blueprint relationships"
        >
          {children}
        </svg>
      </div>
    </div>
  );
}
