import { BrandMark } from "@/components/common/brand-mark";
import { Link } from "react-router-dom";

export function Brand() {
  return (
    <Link
      to="/platform/overview"
      className="flex items-center gap-2.5 rounded-md text-[15px] font-semibold tracking-tight outline-none focus-visible:ring-1 focus-visible:ring-ring"
    >
      <BrandMark className="size-6" />
      Groundplane
    </Link>
  );
}
