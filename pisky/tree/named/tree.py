"""Function to build a named tree from a config tree."""

from __future__ import annotations

from collections.abc import Sequence

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


def _collect_terse_children(node: NamedNode) -> list[NamedNode]:
    """Recursively collect only terse (explicitly named) children."""
    result: list[NamedNode] = []
    for child in node.children:
        if child.terse:
            # Keep this node, recursively filter its children
            result.append(NamedNode(
                name=child.name,
                weight=child.weight,
                children=_collect_terse_children(child),
                metadata=child.metadata,
                terse=True,
            ))
        else:
            # Skip this node, but include its terse descendants
            result.extend(_collect_terse_children(child))
    return result


def terse_tree(root: NodeConfig, name: str = "Root") -> NamedNode:
    """Build a terse tree showing only explicitly named nodes.

    Like named_tree, but filters out intermediate nodes (like WeightedNodeConfig,
    ShuffleConfig, etc.) and only shows nodes wrapped in NamedNodeConfig.

    Args:
        root: The root node config.
        name: Name for the root node (default: "Root").

    Returns:
        A NamedNode tree with only named nodes.

    Example:
        config = AutoSamplingConfig([
            NamedNodeConfig("English", ThreadedConfig(
                WeightedNodeConfig(ShuffleConfig(...), 5.0)
            )),
            NamedNodeConfig("German", WeightedNodeConfig(
                RoundRobinConfig([
                    NamedNodeConfig("DE-News", WeightedNodeConfig(..., 2.0)),
                    NamedNodeConfig("DE-Books", WeightedNodeConfig(..., 1.5)),
                ]),
                1.0,
            )),
        ])

        print(terse_tree(config))
        # Root (8.5)
        # ├── English (5.0)
        # └── German (3.5)
        #     ├── DE-News (2.0)
        #     └── DE-Books (1.5)
    """
    full_tree = named_tree(root, name)
    return NamedNode(
        name=full_tree.name,
        weight=full_tree.weight,
        children=_collect_terse_children(full_tree),
        metadata=full_tree.metadata,
        terse=True,
    )


def format_weight_error(node_name: str, children: Sequence[NodeConfig]) -> str:
    """Format a weight error showing which children have/miss weights.

    Args:
        node_name: Name of the parent node (e.g., "RoundRobinConfig").
        children: Sequence of child NodeConfigs.

    Returns:
        Formatted error message with tree structure.

    Example output:
        RoundRobinConfig has mixed weights:
        ├── WeightedNodeConfig (2.0)
        ├── RecordReaderConfig [path=de.disky] ✗ missing weight
        ├── WeightedNodeConfig (1.5)
        └── RecordReaderConfig [path=es.disky] ✗ missing weight
    """
    lines = [f"{node_name} has mixed weights:"]

    for i, child in enumerate(children):
        is_last = i == len(children) - 1
        connector = "└── " if is_last else "├── "

        # Get child name - use custom name if NamedNodeConfig
        if hasattr(child, '_name'):
            name = child._name  # type: ignore
        else:
            name = child.__class__.__name__

        # Try to get weight safely
        try:
            weight = child.weight
        except Exception:
            weight = None

        # Format the line
        if weight is not None:
            lines.append(f"{connector}{name} ({weight})")
        else:
            lines.append(f"{connector}{name} ✗ missing weight")

    return "\n".join(lines)
