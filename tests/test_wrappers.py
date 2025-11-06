"""Wrapper tests."""

from pathlib import Path

import pytest

from sdag.models import (
    EndNode,
    Graph,
    IfNode,
    InputKwarg,
    LogicalType,
    Node,
    OutputType,
    Parent,
    RootNode,
    TaskNode,
)
from sdag.wrappers import IfWrapper, Pipeline, Task


class MockSDAG:
    """Mocked SDAG for testing purposes.

    Attributes:
        nodes (list[Node]): Nodes.
        branches (list[Node[IfNode]]): Branches.
        dags (list[str]): DAGs.
    """

    def __init__(self):
        """Initialize the mocked sdag."""
        self.nodes: list[Node] = []
        self.branches: list[Node[IfNode]] = []
        self.dags: list[str] = []

    def register(self, node: Node):
        """Register a node.

        Args:
            node (Node): Registered node.
        """
        self.nodes.append(node)

    def get_uid(self) -> str:
        """Retrieve a uid.

        Returns:
            str: uid.
        """
        return "0"

    def push_branch(self, node: Node[IfNode]) -> None:
        """Push a branch.

        Args:
            node (Node[IfNode]): If node.
        """
        self.branches.append(node)

    def set_dag(self, name: str) -> None:
        """Set a dag.

        Args:
            name (str): DAG name.
        """
        self.dags.append(name)

    def get_graph(self) -> Graph:
        """Retrieve a graph.

        Returns:
            Graph: Graph.
        """
        return Graph(
            name="",
            nodes=[
                Node(uid="0", behavior=RootNode(children=["1"])),
                Node(
                    uid="1",
                    parents=[Parent(uid="0", parent_type=LogicalType())],
                    behavior=TaskNode(
                        fname="test",
                        launch_script=Path(),
                        caching=False,
                        retries=0,
                        children=["2"],
                    ),
                ),
                Node(
                    uid="2",
                    parents=[Parent(uid="1", parent_type=LogicalType())],
                    behavior=EndNode(),
                ),
            ],
        )


class TestTask:
    """Task tests."""

    def mock_stage(self) -> None:
        """Mocked stage function."""

    def test_call(self) -> None:
        """Test the task call."""
        sdag = MockSDAG()
        task = Task(
            fn=self.mock_stage,
            caching=True,
            retries=2,
            launch_script=Path(),
            register=sdag.register,
            get_uid=sdag.get_uid,
        )

        argnode = Node(uid="1", behavior=RootNode())
        kwargnode = Node(uid="2", behavior=RootNode())
        static_input = {"a": 1}

        node = task(argnode, kwarg=kwargnode, static_input=static_input)

        assert node.uid == "0"
        assert sdag.nodes[0] is node
        assert node.behavior.caching == task.caching
        assert node.behavior.retries == task.retries
        assert node.behavior.launch_script == task.launch_script
        assert node.behavior.input_kwargs == [
            InputKwarg(key="static_input", value=r'{"a": 1}')
        ]

        assert sorted(node.parents, key=lambda x: x.uid) == [
            Parent(uid="1", parent_type=LogicalType()),
            Parent(uid="2", parent_type=OutputType(key="kwarg")),
        ]

        assert argnode.behavior.children == ["0"]
        assert kwargnode.behavior.children == ["0"]


class TestIfWrapper:
    """If wrapper tests."""

    @pytest.fixture
    def node(self) -> Node[IfNode]:
        """If node.

        Returns:
            Node[IfNode]: If node.
        """
        return Node(uid="0", behavior=IfNode())

    def test_enter(self, node: Node[IfNode]) -> None:
        """Test the context manager enter.

        Args:
            node (Node[IfNode]): Node.
        """
        sdag = MockSDAG()
        if_wrapper = IfWrapper(node=node, push_branch=sdag.push_branch)
        wrapper = if_wrapper.__enter__()

        assert wrapper is if_wrapper
        assert sdag.branches[0] is node
        assert node.behavior.in_context

    def test_exit(self, node: Node[IfNode]) -> None:
        """Test the context manager exit.

        Args:
            node (Node[IfNode]): Node.
        """
        sdag = MockSDAG()
        if_wrapper = IfWrapper(node=node, push_branch=sdag.push_branch)
        with if_wrapper:
            ...

        assert not node.behavior.in_context


class TestPipeline:
    """Test the pipeline wrapper."""

    def test_call(self) -> None:
        """Test the pipeline call."""

        def func():
            """Mocked pipeline function."""

        sdag = MockSDAG()
        parent_node = Node(uid="3", behavior=RootNode())
        pipeline = Pipeline(
            fn=func, set_dag=sdag.set_dag, get_graph=sdag.get_graph
        )
        pipeline(parent_node)
