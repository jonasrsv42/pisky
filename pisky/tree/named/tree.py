"""Function to build a named tree from a config tree."""

from __future__ import annotations

from pisky.tree.named.named_node import NamedNode
from pisky.tree.node import NodeConfig


def named_tree(root: NodeConfig, name: str = "Root") -> NamedNode:
    """Build a named tree from a config tree.

    Traverses the config tree and collects all NamedNodeConfig nodes,
    producing a simplified tree showing names and weights.

    Args:
        root: The root node config.
        name: Name for the root node (default: "Root").

    Returns:
        A NamedNode tree for display.

    Example:
        config = AutoSamplingConfig([
            NamedNodeConfig("English", WeightedNodeConfig(..., 2.0)),
            NamedNodeConfig("German", WeightedNodeConfig(..., 1.0)),
        ])

        print(named_tree(config))
        # Root (3.0)
        # ├── English (2.0)
        # └── German (1.0)
    """
    return NamedNode(
        name=name,
        weight=root.weight,
        children=root.named_children(),
    )
