"""Wrapper tests."""

from pathlib import Path

import pytest

from sdag.models import (
    Artifact,
    ArtifactType,
    EndNode,
    Graph,
    GraphMetadata,
    IfNode,
    InputKwarg,
    LogicalType,
    Node,
    OneOfNode,
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
            meta=GraphMetadata(name="test"),
            nodes=[
                Node(uid="0", behavior=RootNode()),
                Node(
                    uid="1",
                    parents=[Parent(uid="0", parent_type=LogicalType())],
                    behavior=TaskNode(
                        fname="test",
                        launch_script=Path(),
                        caching=False,
                        retries=0,
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

    def mock_stage_artifact(self, a: Artifact, b: str) -> None:
        """Mocked stage function with artifacts."""

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
            InputKwarg(key="static_input", value={"a": 1})
        ]

        assert sorted(node.parents, key=lambda x: x.uid) == [
            Parent(uid="1", parent_type=LogicalType()),
            Parent(uid="2", parent_type=OutputType(key="kwarg")),
        ]

    def test_call_artifact(self) -> None:
        """Test the task call."""
        sdag = MockSDAG()
        task = Task(
            fn=self.mock_stage_artifact,
            caching=False,
            retries=1,
            launch_script=Path(),
            register=sdag.register,
            get_uid=sdag.get_uid,
        )

        artifact_path = "/path/to/artifact"
        parent = Node(uid="1", behavior=RootNode())
        parent.register_artifact("artifact", path=Path(artifact_path))
        node = task(a=parent.artifacts["artifact"], b="/path/to/artifact")

        assert node.uid == "0"
        assert sdag.nodes[0] is node
        assert node.behavior.caching == task.caching
        assert node.behavior.retries == task.retries
        assert node.behavior.launch_script == task.launch_script
        assert node.behavior.input_kwargs == [
            InputKwarg(key="b", value="/path/to/artifact")
        ]
        assert node.parents == [
            Parent(
                uid="1",
                parent_type=ArtifactType(
                    key="a", name="artifact", path=Path(artifact_path)
                ),
            ),
        ]


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

    def test_compile(self) -> None:
        """Test mocked pipeline compilation."""

        def func() -> None:
            """Test the pipeline call."""

        sdag = MockSDAG()
        pipeline = Pipeline(
            fn=func, set_dag=sdag.set_dag, get_graph=sdag.get_graph
        )
        graph = pipeline.compile()
        assert isinstance(graph, Graph)

    def test_compile_with_input(self) -> None:
        """Test mocked pipeline compilation with input arguments."""

        def func(a: str) -> None:
            """Test the pipeline call."""

        sdag = MockSDAG()
        pipeline = Pipeline(
            fn=func, set_dag=sdag.set_dag, get_graph=sdag.get_graph
        )
        graph = pipeline.compile({"a": 3})
        assert isinstance(graph, Graph)

    def test_bubbles_up_oneof(self) -> None:
        graph = Graph(
            meta=GraphMetadata(name="graph"),
            nodes=[
                Node(
                    uid="0",
                    parents=[Parent(uid="1", parent_type=LogicalType())],
                    behavior=OneOfNode(),
                ),
                Node(
                    uid="1",
                    behavior=TaskNode(
                        fname="fname",
                        launch_script=Path("submit.sh"),
                        caching=True,
                        retries=0,
                    ),
                ),
            ],
        )

        def func() -> None:
            """Test the pipeline call."""

        sdag = MockSDAG()
        pipeline = Pipeline(
            fn=func, set_dag=sdag.set_dag, get_graph=sdag.get_graph
        )
        pipeline._bubbles_up_oneof(uids={"0"}, graph=graph)
        for node in graph.nodes:
            assert node.output_used

    def test_mark_nodes_requiring_output(self) -> None:
        graph = Graph(
            meta=GraphMetadata(name="graph"),
            nodes=[
                Node(
                    uid="0",
                    parents=[Parent(uid="1", parent_type=LogicalType())],
                    behavior=OneOfNode(),
                ),
                Node(
                    uid="1",
                    behavior=TaskNode(
                        fname="fname",
                        launch_script=Path("submit.sh"),
                        caching=True,
                        retries=0,
                    ),
                ),
                Node(
                    uid="2",
                    parents=[Parent(uid="0", parent_type=OutputType(key="a"))],
                    behavior=TaskNode(
                        fname="fname",
                        launch_script=Path("submit.sh"),
                        caching=True,
                        retries=0,
                    ),
                ),
            ],
        )

        def func() -> None:
            """Test the pipeline call."""

        sdag = MockSDAG()
        pipeline = Pipeline(
            fn=func, set_dag=sdag.set_dag, get_graph=sdag.get_graph
        )
        pipeline._mark_nodes_requiring_output(graph)
        nodemap = {node.uid: node for node in graph.nodes}

        assert nodemap["0"].output_used
        assert nodemap["1"].output_used
        assert not nodemap["2"].output_used
