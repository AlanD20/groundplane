"""Checkout-filesystem headroom; this check does not reserve shared storage."""
from pathlib import Path
import shutil

BUILD_HEADROOM = 10 << 30
TRANSFER_HEADROOM = 2 << 30


def require_capacity(root: Path, minimum: int = BUILD_HEADROOM) -> None:
    available = shutil.disk_usage(root).free
    if available < minimum:
        raise RuntimeError(
            f"insufficient free space on {root}: {available / (1 << 30):.1f} GiB; "
            f"at least {minimum / (1 << 30):.1f} GiB required. "
            "Reclaim only verified disposable artifacts or use a separate builder."
        )
