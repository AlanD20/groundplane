import { Layers } from "lucide-react";
import { Link } from "react-router-dom";

export function Brand() {
  return (
    <Link
      to="/platform/overview"
      className="flex items-center gap-2.5 rounded-md text-[15px] font-semibold tracking-tight outline-none focus-visible:ring-1 focus-visible:ring-ring"
    >
      <Layers className="size-5 text-primary" strokeWidth={1.6} />
      Groundplane
    </Link>
  );
}
