"""Model tests."""

import logging
from datetime import datetime
from pathlib import Path

import pytest

from sdag.exceptions import EndNotFoundError, RootNotFoundError
from sdag.models import (
    Artifact,
    ArtifactType,
    BranchType,
    EndNode,
    Graph,
    GraphMetadata,
    LogicalType,
    Node,
    OutputType,
    Parent,
    RootNode,
    TaskNode,
    get_compile_settings,
)


class TestNode:
    """Test methods common to all node types."""

    @pytest.fixture
    def node(self) -> Node[RootNode]:
        """Node.

        Returns:
            Node[RootNode]: Node.
        """
        return Node(uid="0", behavior=RootNode())

    def test_register_artifact(self, node: Node[RootNode]) -> None:
        """Test the artifact registration.

        Args:
            node (Node[RootNode]): Node.
        """
        # Otherwise we get inconsistent results
        get_compile_settings.cache_clear()
        node.register_artifact(key="artifact", path=Path())
        artifacts = node.artifacts

        assert artifacts["artifact"].key == "artifact"
        assert artifacts["artifact"].path == Path()
        assert artifacts["artifact"].node is node
        assert node.output_artifacts == [
            Artifact(name="artifact", path=Path())
        ]

    def test_add_logical_edge(self, node: Node[RootNode]) -> None:
        """Test logical edge addition.

        Args:
            node (Node[RootNode]): Node.
        """
        node.add_logical_edge(parent_uid="1")
        assert node.parents == [Parent(uid="1", parent_type=LogicalType())]

    def test_add_output_edge(self, node: Node[RootNode]) -> None:
        """Test output edge addition.

        Args:
            node (Node[RootNode]): Node.
        """
        node.add_output_edge(parent_uid="1", key="key")
        assert node.parents == [
            Parent(uid="1", parent_type=OutputType(key="key"))
        ]

    def test_add_artifact_edge(self, node: Node[RootNode]) -> None:
        """Test artifact edge addition.

        Args:
            node (Node[RootNode]): Node.
        """
        node.add_artifact_edge(
            parent_uid="1", key="key", name="artifact", path=Path()
        )
        assert node.parents == [
            Parent(
                uid="1",
                parent_type=ArtifactType(
                    key="key", name="artifact", path=Path()
                ),
            )
        ]

    def test_add_branch_edge(self, node: Node[RootNode]) -> None:
        """Test branch edge addition.

        Args:
            node (Node[RootNode]): Node.
        """
        node.add_branch_edge(parent_uid="1", branch=False)
        assert node.parents == [
            Parent(uid="1", parent_type=BranchType(branch=False))
        ]

    def test_join_artifact_containers(self, node: Node[RootNode]) -> None:
        """Test the artifact container join.

        Args:
            node (Node[RootNode]): Node.
        """
        end = Node(uid="1", behavior=EndNode())
        end.register_artifact(key="a", path=Path())
        node.join_artifact_containers(end._artifact_containers)

        assert "a" in node._artifact_containers
        assert not node.output_artifacts

    def test_join_overlapping_artifact_containers(
        self, node: Node[RootNode], caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test the merge of overlapping artifacts.

        A log must be thrown if artifacts with the same
        name are merged in the end node.

        Args:
            node (Node[RootNode]): Node.
            caplog (pytest.LogCaptureFixture): Fixture to capture logs.
        """
        node.register_artifact(key="a", path=Path())
        end = Node(uid="1", behavior=EndNode())
        end.register_artifact(key="a", path=Path())

        caplog.set_level(logging.INFO)
        node.join_artifact_containers(end._artifact_containers)

        assert len(caplog.records) == 1
        for record in caplog.records:
            assert record.levelname == "INFO"
        assert "duplicate artifact" in caplog.text


class TestGraph:
    """Test graph methods."""

    @pytest.fixture
    def graph(self) -> Graph:
        """Graph.

        Returns:
            Graph: Graph.
        """
        return Graph(
            meta=GraphMetadata(
                name="graph",
                creation_dt=datetime(1920, 1, 1, 9, 20, 20),
            ),
            nodes=[
                Node(uid="root", behavior=RootNode()),
                Node(
                    uid="end",
                    parents=[Parent(uid="task", parent_type=LogicalType())],
                    behavior=EndNode(),
                ),
                Node(
                    uid="task",
                    parents=[Parent(uid="root", parent_type=LogicalType())],
                    behavior=TaskNode(
                        fname="test",
                        launch_script=Path(),
                        caching=False,
                        retries=0,
                    ),
                ),
            ],
        )

    def test_get_root(self, graph: Graph) -> None:
        """Test the root node retrieval.

        Args:
            graph (Graph): Graph.
        """
        root = graph.get_root()
        assert root.uid == "root"

    def test_get_end(self, graph: Graph) -> None:
        """Test the end node retrieval.

        Args:
            graph (Graph): Graph.
        """
        end = graph.get_end()
        assert end.uid == "end"

    def test_fail_get_root(self, graph: Graph) -> None:
        """Method fails if a root node is not found.

        Args:
            graph (Graph): Graph.
        """
        graph.nodes = [n for n in graph.nodes if n.uid != "root"]
        with pytest.raises(RootNotFoundError):
            graph.get_root()

    def test_find_nodes_with_children(self, graph: Graph) -> None:
        """Check the children node uid retrieval.

        Args:
            graph (Graph): Graph.
        """
        parent_uids = graph.find_nodes_with_children()
        assert parent_uids == {"root", "task"}

    def test_fail_get_end(self, graph: Graph) -> None:
        """Method fails if a end node is not found.

        Args:
            graph (Graph): Graph.
        """
        # Fake edge to the end node
        graph.nodes[0].parents.append(
            Parent(uid="end", parent_type=LogicalType())
        )

        with pytest.raises(EndNotFoundError):
            graph.get_end()
