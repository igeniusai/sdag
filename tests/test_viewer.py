"""Viewer tests."""

import json
from pathlib import Path

import pytest

from sdag import DAGViewer
from sdag.models import (
    Artifact,
    ArtifactType,
    EndNode,
    Graph,
    IfNode,
    LogicalType,
    Node,
    NodeUnion,
    OneOfNode,
    OutputType,
    Parent,
    RootNode,
    TaskNode,
)


class TestDAGViewer:
    """Test the viewer."""

    def test_artifact_id(self) -> None:
        """Test the artifact ID calculation."""
        viewer = DAGViewer()
        assert viewer._get_artifact_id() == "_artifact_0_"
        assert viewer._artifact_id == 1

    @pytest.mark.parametrize(
        argnames=("node", "color", "size", "caption"),
        argvalues=[
            (
                Node(
                    uid="1",
                    parents=[],
                    behavior=TaskNode(
                        fname="fname",
                        launch_script=Path("script.sh"),
                        cmd="sbatch",
                        mode="ext",
                        caching=False,
                        retries=0,
                        input_kwargs=[],
                    ),
                ),
                "red",
                5,
                "fname",
            ),
            (
                Node(
                    uid="1",
                    parents=[],
                    behavior=RootNode(),
                ),
                "orange",
                10,
                "",
            ),
            (
                Node(
                    uid="1",
                    parents=[],
                    behavior=EndNode(),
                ),
                "yellow",
                15,
                "",
            ),
            (
                Node(
                    uid="1",
                    parents=[],
                    behavior=IfNode(),
                ),
                "green",
                20,
                "If",
            ),
            (
                Node(
                    uid="1",
                    parents=[],
                    behavior=OneOfNode(),
                ),
                "blue",
                25,
                "OneOf",
            ),
        ],
    )
    def test_get_viznode(
        self, node: Node, color: str, size: int, caption: str
    ) -> None:
        """Test the visualization node determination.

        Args:
            node (Node): Node.
            color (str): Expected node color.
            size (int): Expected node size.
            caption (str): Expected node caption.
        """
        viewer = DAGViewer(
            task_size=5,
            task_color="red",
            root_size=10,
            root_color="orange",
            end_size=15,
            end_color="yellow",
            if_size=20,
            if_color="green",
            oneof_size=25,
            oneof_color="blue",
        )

        viznode = viewer._get_viznode(node)
        assert str(viznode.color) == color
        assert viznode.size == size
        assert viznode.caption == caption

    @pytest.mark.parametrize(
        argnames=("parent", "source"),
        argvalues=[
            (Parent(uid="0", parent_type=LogicalType()), "0"),
            (Parent(uid="0", parent_type=OutputType(key="output")), "0"),
            (
                Parent(
                    uid="0",
                    parent_type=ArtifactType(
                        key="output", name="name", path=Path()
                    ),
                ),
                "_artifact_0_",
            ),
        ],
    )
    def test_get_relationship(self, parent: Parent, source: str) -> None:
        """Test the relationship retrieval.

        Args:
            parent (Parent): Node parent in the graph.
            source (str): Expected source id.
        """
        child_uid = "1"
        viewer = DAGViewer()
        viewer._artifact_dict["0"]["name"] = "_artifact_0_"
        relationship = viewer._get_relationship(uid=child_uid, parent=parent)

        assert relationship.target == child_uid
        assert relationship.source == source

    def test_get_relationships(self) -> None:
        """The relationships."""
        viewer = DAGViewer()
        viewer._artifact_dict["0"]["artifact"] = "_artifact_0_"
        nodes: list[NodeUnion] = [
            Node(
                uid="0",
                parents=[Parent(uid="1", parent_type=LogicalType())],
                behavior=TaskNode(
                    fname="fname",
                    launch_script=Path("script.sh"),
                    cmd="sbatch",
                    mode="ext",
                    caching=False,
                    retries=0,
                    input_kwargs=[],
                ),
                output_artifacts=[Artifact(name="artifact", path=Path())],
            ),
        ]

        relationships = viewer._get_relationships(nodes)
        assert len(relationships) == 2

    def test_get_nodes(self) -> None:
        """Test nodes."""
        viewer = DAGViewer()
        nodes: list[NodeUnion] = [
            Node(
                uid="0",
                parents=[Parent(uid="1", parent_type=LogicalType())],
                behavior=TaskNode(
                    fname="fname",
                    launch_script=Path("script.sh"),
                    cmd="sbatch",
                    mode="ext",
                    caching=False,
                    retries=0,
                    input_kwargs=[],
                ),
                output_artifacts=[Artifact(name="artifact", path=Path())],
            ),
        ]

        relationships = viewer._get_nodes(nodes)

        assert len(relationships) == 2
        assert viewer._artifact_dict["0"]["artifact"] == "_artifact_0_"

    def test_parse_dag(self, tmp_path: Path) -> None:
        """Test an empty DAG parsing.

        Args:
            tmp_path (Path): Temporary path fixture.
        """
        dag = {
            "meta": {"name": "pipeline"},
            "nodes": [],
        }
        path = tmp_path / "graph.json"
        with path.open("w") as f:
            json.dump(dag, f)

        viewer = DAGViewer()
        graph = viewer._parse_dag(path)
        assert isinstance(graph, Graph)

    def test_view(self, tmp_path: Path) -> None:
        """End-to-end empty dag view.

        Args:
            tmp_path (Path): Temporary path fixture.
        """
        from IPython.display import HTML

        dag = {
            "meta": {"name": "pipeline"},
            "nodes": [],
        }
        path = tmp_path / "graph.json"
        with path.open("w") as f:
            json.dump(dag, f)

        viewer = DAGViewer()
        html = viewer.view(path)

        assert isinstance(html, HTML)
