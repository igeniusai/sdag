"""Graph optimizations."""

from typing import cast

from sdag.models import Graph, Node, NodeUnion, TaskNode


class GraphJoiner:
    """Join cached nodes.

    Simple implementation to join similar nodes. O(N^2) at the
    moment. It can be brought to O(N) by creating a unique hash
    for the input values but it's OK to keep it simple.

    To be fused together, two nodes must:
    - Be the same task
    - Have the same input kwargs
    - Have no dynamic input
    - Do not depend on each other

    These conditions make the process fairly safe.
    """

    def join_nodes(self, graph: Graph) -> None:
        """Join graph nodes.

        Args:
            graph (Graph): Compiled graph, modified in place.
        """
        swaps = self._find_swaps(graph.nodes)
        fused_nodes = self._fuse_nodes(graph.nodes, swaps)
        graph.nodes = fused_nodes

    def _find_swaps(self, nodes: list[NodeUnion]) -> dict[str, str]:
        """Find nodes to be joined.

        Args:
            nodes (list[NodeUnion]): Graph nodes.

        Returns:
            dict[str, str]: map between node uids to be joined.
        """
        candidates: dict[str, Node] = {}
        swaps: dict[str, str] = {}

        for node in nodes:
            if not self._is_node_eligible(node):
                continue

            node = cast(Node[TaskNode], node)
            for candidate in candidates.values():
                if self._match(node, candidate):
                    swaps[node.uid] = candidate.uid
                    break
            else:
                candidates[node.uid] = node

        return swaps

    def _is_node_eligible(self, node: NodeUnion) -> bool:
        """Check if the node can be selected.

        Args:
            node (NodeUnion): Node.

        Returns:
            bool: True if the node could be selected.
        """
        return (
            node.behavior.type == "TaskNode"
            and node.behavior.caching
            and all(p.parent_type.type != "Output" for p in node.parents)
        )

    def _fuse_nodes(
        self, nodes: list[NodeUnion], swaps: dict[str, str]
    ) -> list[NodeUnion]:
        """Join two nodes together.

        Args:
            nodes (list[NodeUnion]): Graph nodes.
            swaps (dict[str, str]): Map between uids to be joined.

        Returns:
            list[NodeUnion]: Graph with fused nodes.
        """
        swap_nodes: dict[str, Node] = {
            n.uid: n for n in nodes if n.uid in swaps.values()
        }

        fused_nodes: list[NodeUnion] = []
        for node in nodes:
            self._replace_parent_uids(node, swaps)
            if node.uid not in swaps:
                fused_nodes.append(node)
                continue

            swap_uid = swaps[node.uid]
            swap_node = swap_nodes[swap_uid]
            self._merge_nodes(node, swap_node)

        return fused_nodes

    def _match(self, node: Node[TaskNode], swap: Node[TaskNode]) -> bool:
        """Check if two nodes can be joined.

        Function name and input kwargs must correspond.

        Args:
            node (Node[TaskNode]): Node to be tested.
            swap (Node[TaskNode]): Replacement candidate.

        Returns:
            bool: True if the nodes match.
        """
        if (
            node.behavior.fname != swap.behavior.fname
            or any(parent.uid == swap.uid for parent in node.parents)
            or any(parent.uid == node.uid for parent in swap.parents)
        ):
            return False

        node_input = {kw.key: kw.value for kw in node.behavior.input_kwargs}
        swap_input = {kw.key: kw.value for kw in swap.behavior.input_kwargs}
        return node_input == swap_input

    def _replace_parent_uids(self, node: Node, swaps: dict[str, str]) -> None:
        """Fuse edges.

        Args:
            node (Node): Node.
            swaps (dict[str, str]): Map between uids to be joined.
        """
        for parent in node.parents:
            if parent.uid in swaps:
                parent.uid = swaps[parent.uid]

    def _merge_nodes(self, node: Node, swap: Node[TaskNode]) -> None:
        """Merge nodes.

        Args:
            node (Node): Node to be merged.
            swap (Node[TaskNode]): Node that will be kept.
        """
        for parent in node.parents:
            if all(parent != swap_parent for swap_parent in swap.parents):
                swap.parents.append(parent)
