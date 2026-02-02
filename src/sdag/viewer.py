"""DAG viewer.

It requires the `neo4j_viz` library to be installed. It is
meant to be used in a notebook.
"""

import json
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from sdag.models import Graph, NodeUnion, ParentUnion

if TYPE_CHECKING:
    from IPython.display import HTML
    from neo4j_viz import Node as VizNode
    from neo4j_viz import Relationship


class DAGViewer:
    """DAG viewer.

    Attributes:
        task_size (int, optional): Task node size.
        task_color (str, optional): Task color.
        root_size (int, optional): Root node size.
        root_color (str, optional): Root color.
        end_size (int, optional): End node size.
        end_color (str, optional): End color.
        if_size (int, optional): If node size.
        if_color (str, optional): If color.
        oneof_size (int, optional): OneOf node size.
        oneof_color (str, optional): OneOf color.
        artifact_size (int, optional): Artifact node size.
        artifact_color (str, optional): Artifact color.
        caption_size (Literal[1, 2, 3], optional): Caption size.
        _artifact_id (int): Used to generate unique,
            reproducible artifact ids.
        _artifact_dict (dict[str, dict[str, str]]):
            `node uid -> artifact name -> artifact id` map.
    """

    def __init__(
        self,
        task_size: int = 50,
        task_color: str = "#3E7B7C",
        root_size: int = 15,
        root_color: str = "#F5F7FA",
        end_size: int = 15,
        end_color: str = "#F5F7FA",
        if_size: int = 30,
        if_color: str = "#D4A5A5",
        oneof_size: int = 30,
        oneof_color: str = "#D4A5A5",
        artifact_size: int = 40,
        artifact_color: str = "#F0D48D",  # "#E0D7C6",
        caption_size: Literal[1, 2, 3] = 2,
    ) -> None:
        """Initialize the viewer.

        Args:
            task_size (int, optional): Task node size. Defaults to 50.
            task_color (str, optional): Task color.
                Defaults to "#3E7B7C".
            root_size (int, optional): Root node size. Defaults to 15.
            root_color (str, optional): Root color. Defaults to "#F5F7FA".
            end_size (int, optional): End node size. Defaults to 15.
            end_color (str, optional): End color. Defaults to "#F5F7FA".
            if_size (int, optional): If node size. Defaults to 30.
            if_color (str, optional): If color. Defaults to "#D4A5A5".
            oneof_size (int, optional): OneOf node size. Defaults to 30.
            oneof_color (str, optional): OneOf color. Defaults to "#D4A5A5".
            artifact_size (int, optional): Artifact node size.
                Defaults to 40.
            artifact_color (str, optional): Artifact color.
                Defaults to "#F0D48D".
            caption_size (Literal[1, 2, 3], optional): Caption size.
                Defaults to 2.
        """
        self.task_size = task_size
        self.task_color = task_color
        self.root_size = root_size
        self.root_color = root_color
        self.end_size = end_size
        self.end_color = end_color
        self.if_size = if_size
        self.if_color = if_color
        self.oneof_size = oneof_size
        self.oneof_color = oneof_color
        self.artifact_size = artifact_size
        self.artifact_color = artifact_color
        self.caption_size = caption_size

        self._artifact_id = 0
        self._artifact_dict: dict[str, dict[str, str]] = defaultdict(dict)

    def view(self, path: str | Path) -> "HTML":
        """View the DAG graph in a notebook cell.

        Args:
            path (str | Path): Compiled pipeline path.

        Returns:
            HTML: IPython HTML graph.
        """
        from neo4j_viz import Layout, VisualizationGraph

        self._reset_artifact_id()

        graph = self._parse_dag(path)
        nodes = self._get_nodes(graph.nodes)
        relationships = self._get_relationships(graph.nodes)
        vg = VisualizationGraph(nodes=nodes, relationships=relationships)

        return vg.render(layout=Layout.HIERARCHICAL)

    def _parse_dag(self, path: str | Path) -> Graph:
        """Parse a DAG graph.

        Args:
            path (str | Path): Compiled pipeline path.

        Returns:
            Graph: Parsed DAG.
        """
        with Path(path).open() as f:
            data = json.load(f)

        return Graph.model_validate(data)

    def _get_nodes(self, nodes: list[NodeUnion]) -> "list[VizNode]":
        """Get the graph nodes.

        Args:
            nodes (list[NodeUnion]): Graph nodes.

        Returns:
            list[VizNode]: Graph nodes.
        """
        from neo4j_viz import Node as VizNode

        viznodes: list[VizNode] = []

        for node in nodes:
            viznode = self._get_viznode(node)
            viznodes.append(viznode)

            for artifact in node.output_artifacts:
                aid = self._get_artifact_id()
                self._artifact_dict[node.uid][artifact.name] = aid
                viznodes.append(
                    VizNode(
                        id=aid,
                        caption=artifact.name,
                        size=self.artifact_size,
                        color=self.artifact_color,
                        caption_size=self.caption_size,
                    )  # type: ignore
                )

        return viznodes

    def _get_relationships(
        self, nodes: list[NodeUnion]
    ) -> "list[Relationship]":
        """Get graph edges.

        Args:
            nodes (list[NodeUnion]): DAG nodes.

        Returns:
            list[Relationship]: Graph relationships.
        """
        from neo4j_viz import Relationship

        relationships: list[Relationship] = []
        for node in nodes:
            for parent in node.parents:
                relationship = self._get_relationship(node.uid, parent)
                relationships.append(relationship)

            for artifact in node.output_artifacts:
                node_artifacts = self._artifact_dict[node.uid]
                target = node_artifacts[artifact.name]
                relationships.append(
                    Relationship(
                        source=node.uid,
                        target=target,
                        caption_size=self.caption_size,
                    )  # type: ignore
                )

        return relationships

    def _get_viznode(self, node: NodeUnion) -> "VizNode":
        """Get a node based on the node type.

        Args:
            node (NodeUnion): DAG node.

        Raises:
            ValueError: The node type is unknown.

        Returns:
            VizNode: Node.
        """
        from neo4j_viz import Node as VizNode

        match node.behavior.type:
            case "TaskNode":
                color = self.task_color
                size = self.task_size
                caption = node.behavior.name  # type: ignore

            case "IfNode":
                color = self.if_color
                size = self.if_size
                caption = "If"

            case "OneOfNode":
                color = self.oneof_color
                size = self.oneof_size
                caption = "OneOf"

            case "RootNode":
                color = self.root_color
                size = self.root_size
                caption = ""

            case "EndNode":
                color = self.end_color
                size = self.end_size
                caption = ""

            case _:
                msg = f"Unknown node type {node.behavior.type}"
                raise ValueError(msg)

        return VizNode(
            id=node.uid,
            caption=caption,
            size=size,
            color=color,
            caption_size=self.caption_size,
        )  # type: ignore

    def _get_relationship(
        self, uid: str, parent: ParentUnion
    ) -> "Relationship":
        """Get the relationship based on the relation type.

        Args:
            uid (str): Target node uid.
            parent (ParentUnion): Parent.

        Raises:
            ValueError: Unknown parent type.

        Returns:
            Relationship: Graph edge.
        """
        from neo4j_viz import Relationship

        match parent.parent_type.type:
            case "Artifact":
                artifact_name = parent.parent_type.name  # type: ignore
                source = self._artifact_dict[parent.uid][artifact_name]

            case "Logical" | "Branch" | "Output":
                source = parent.uid

            case _:
                msg = f"Unknown relation {parent.parent_type.type}"
                raise ValueError(msg)

        return Relationship(
            source=source,  # type: ignore
            target=uid,
        )

    def _get_artifact_id(self) -> str:
        """Get a unique artifact id for the graph.

        Returns:
            str: Artifact id.
        """
        uid = f"_artifact_{self._artifact_id}_"
        self._artifact_id += 1
        return uid

    def _reset_artifact_id(self) -> None:
        """Reset the artifact id."""
        self._artifact_id = 0
