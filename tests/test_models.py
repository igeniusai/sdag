"""Model tests."""

from datetime import datetime
from pathlib import Path

import pytest

from sdag.exceptions import EndNotFoundError, RootNotFoundError
from sdag.models import (
    EndNode,
    Graph,
    IfNode,
    LogicalType,
    Node,
    OneOfNode,
    OutputType,
    Parent,
    RootNode,
    TaskNode,
)


class TestTaskNode:
    """Task node tests."""

    @pytest.fixture
    def task_node(self) -> TaskNode:
        """Task node.

        Returns:
            TaskNode: Task node.
        """
        return TaskNode(
            fname="fname", launch_script=Path(), caching=False, retries=0
        )

    def test_add_child(self, task_node: TaskNode) -> None:
        """Test the child addition.

        Args:
            task_node (TaskNode): Task node.
        """
        task_node.add_child("0")
        assert task_node.children == ["0"]

    def test_is_leaf(self, task_node: TaskNode) -> None:
        """This node is a leaf.

        Args:
            task_node (TaskNode): Task node.
        """
        assert task_node.is_leaf()

    def test_is_not_leaf(self, task_node: TaskNode) -> None:
        """After a child addition, it's not a leaf anymore.

        Args:
            task_node (TaskNode): Task node.
        """
        task_node.add_child("0")
        assert not task_node.is_leaf()


class TestIfNode:
    """If node tests."""

    @pytest.fixture
    def if_node(self) -> IfNode:
        """If node.

        Returns:
            IfNode: If node.
        """
        return IfNode()

    @pytest.mark.parametrize(
        argnames=("branch", "true_branch", "false_branch"),
        argvalues=[
            (True, ["0"], []),
            (False, [], ["0"]),
        ],
    )
    def test_add_child(
        self,
        if_node: IfNode,
        branch: bool,
        true_branch: list[str],
        false_branch: list[str],
    ) -> None:
        """Test the child addition.

        Args:
            if_node (IfNode): If node.
            branch (bool): Current branch.
            true_branch (list[str]): Branch if the condition is True.
            false_branch (list[str]): Branch if the condition is False.
        """
        if_node.branch = branch
        if_node.add_child("0")
        assert if_node.true_branch == true_branch
        assert if_node.false_branch == false_branch


class TestOneOfNode:
    """OneOf node tests."""

    @pytest.fixture
    def oneof_node(self) -> OneOfNode:
        """OneOf node.

        Returns:
            OneOfNode: OneOf node.
        """
        return OneOfNode()

    def test_add_child(self, oneof_node: OneOfNode) -> None:
        """Test the child addition.

        Args:
            oneof_node (OneOfNode): OneOf node.
        """
        oneof_node.add_child("0")
        assert oneof_node.children == ["0"]

    def test_is_leaf(self, oneof_node: OneOfNode) -> None:
        """Node is leaf.

        Args:
            oneof_node (OneOfNode): OneOf node.
        """
        assert oneof_node.is_leaf()

    def test_is_not_leaf(self, oneof_node: OneOfNode) -> None:
        """Node is not a leaf anymore after the child addition.

        Args:
            oneof_node (OneOfNode): OneOf node.
        """
        oneof_node.add_child("0")
        assert not oneof_node.is_leaf()


class TestRootNode:
    """Test the root node."""

    @pytest.fixture
    def root_node(self) -> RootNode:
        """Root node."""
        return RootNode()

    def test_add_child(self, root_node: RootNode) -> None:
        """Test the child addition.

        Args:
            root_node (RootNode): Root node.
        """
        root_node.add_child("0")
        assert root_node.children == ["0"]

    def test_is_leaf(self, root_node: RootNode) -> None:
        """The node is a leaf at the moment.

        Args:
            root_node (RootNode): Root node.
        """
        assert root_node.is_leaf()

    def test_is_not_leaf(self, root_node: RootNode) -> None:
        """Node is not a leaf anymore after the child addition.

        Args:
            root_node (RootNode): Root node.
        """
        root_node.add_child("0")
        assert not root_node.is_leaf()


class TestEndNode:
    """End node tests."""

    @pytest.fixture
    def end_node(self) -> EndNode:
        """End node.

        Returns:
            EndNode: End node.
        """
        return EndNode()

    def test_add_child(self, end_node: EndNode) -> None:
        """Test the child addition.

        Args:
            end_node (EndNode): End node.
        """
        end_node.add_child("0")
        assert end_node.children == ["0"]

    def test_is_leaf(self, end_node: EndNode) -> None:
        """Node is a leaf.

        Args:
            end_node (EndNode): End node.
        """
        assert end_node.is_leaf()

    def test_is_not_leaf(self, end_node: EndNode) -> None:
        """Node is not a leaf anymore after the child addition.

        Args:
            end_node (EndNode): End node.
        """
        end_node.add_child("0")
        assert not end_node.is_leaf()


class TestNode:
    """Test methods common to all node types."""

    @pytest.fixture
    def node(self) -> Node[RootNode]:
        """Node.

        Returns:
            Node[RootNode]: Node.
        """
        return Node(uid="0", behavior=RootNode())

    def test_is_leaf(self, node: Node[RootNode]) -> None:
        """Test if the node is a leaf.

        Args:
            node (Node[RootNode]): Node.
        """
        assert node.is_leaf()

    def test_add_edge(self, node: Node[RootNode]) -> None:
        """Test edge addition.

        Args:
            node (Node[RootNode]): Node.
        """
        child = Node(uid="1", behavior=EndNode())
        node.add_output_edge(child, key="test")

        assert child.parents == [
            Parent(uid="0", parent_type=OutputType(key="test"))
        ]
        assert node.behavior.children == ["1"]


class TestGraph:
    """Test graph methods."""

    @pytest.fixture
    def graph(self) -> Graph:
        """Graph.

        Returns:
            Graph: Graph.
        """
        return Graph(
            name="graph",
            creation_dt=datetime(1920, 1, 1, 9, 20, 20),
            nodes=[
                Node(uid="root", behavior=RootNode(children=["test"])),
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
                        children=["end"],
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

    def test_fail_get_end(self, graph: Graph) -> None:
        """Method fails if a end node is not found.

        Args:
            graph (Graph): Graph.
        """
        graph.nodes = [n for n in graph.nodes if n.uid != "end"]
        with pytest.raises(EndNotFoundError):
            graph.get_end()
