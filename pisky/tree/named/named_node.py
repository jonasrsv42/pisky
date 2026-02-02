"""NamedNode dataclass for tree display."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any


@dataclass
class NamedNode:
    """A node in the named tree for display purposes.

    Example output from __str__:
        Root (6.0)
        ├── ShuffleConfig (2.0) [buffer_size=1000, seed=42]
        │   └── RecordReaderConfig (None) [path=en.disky]
        └── WeightedNodeConfig (1.0) [weight=1.0]
            └── RecordReaderConfig (None) [path=de.disky]
    """

    name: str
    weight: float | None
    children: Sequence[NamedNode]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    terse: bool = False

    def __str__(self) -> str:
        """Return a tree-formatted string representation."""
        return self._to_string(prefix="", is_root=True, total_weight=self.weight)

    def _to_string(
        self,
        prefix: str,
        is_root: bool,
        is_last: bool = True,
        total_weight: float | None = None,
    ) -> str:
        """Build string representation recursively."""
        # Format weight with percentage
        if self.weight is not None:
            if total_weight is not None and total_weight > 0:
                pct = (self.weight / total_weight) * 100
                weight_str = f" ({self.weight}, {pct:.1f}%)"
            else:
                weight_str = f" ({self.weight})"
        else:
            weight_str = ""

        # Format metadata
        if self.metadata:
            meta_items = ", ".join(f"{k}={v}" for k, v in self.metadata.items())
            meta_str = f" [{meta_items}]"
        else:
            meta_str = ""

        # Build current line
        if is_root:
            line = f"{self.name}{weight_str}{meta_str}\n"
            child_prefix = ""
        else:
            connector = "└── " if is_last else "├── "
            line = f"{prefix}{connector}{self.name}{weight_str}{meta_str}\n"
            child_prefix = prefix + ("    " if is_last else "│   ")

        # Add children
        for i, child in enumerate(self.children):
            is_last_child = i == len(self.children) - 1
            line += child._to_string(
                child_prefix, is_root=False, is_last=is_last_child, total_weight=total_weight
            )

        return line
