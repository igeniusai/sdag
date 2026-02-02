"""Wrapper tests."""

import inspect
import os
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import pytest
from pytest_mock import MockerFixture

from sdag.exceptions import KwargNotFoundError
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
    OutputType,
    Parent,
    RootNode,
    TaskNode,
    get_compile_settings,
)
from sdag.wrappers import IfWrapper, Pipeline, Task, validate_kwargs


class TestKwargValidation:
    """Test the input kwarg validation."""

    @pytest.mark.parametrize(
        argnames=("fn", "kwargs"),
        argvalues=[
            (lambda a: ..., {}),  # noqa: ARG005
            (lambda: ..., {"a": 1}),
            (lambda a, **kw: ..., {}),  # noqa: ARG005
        ],
    )
    def test_fail_kwargs_validation(
        self, fn: Callable[..., None], kwargs: dict[str, Any]
    ) -> None:
        """kwarg validation must fail.

        Args:
            fn (Callable[..., None]): Task function.
            kwargs (dict[str, Any]): Input kwargs.
        """
        sig = inspect.signature(fn)
        with pytest.raises(KwargNotFoundError):
            validate_kwargs(sig, kwargs)

    @pytest.mark.parametrize(
        argnames=("fn", "kwargs"),
        argvalues=[
            (lambda: ..., {}),
            (lambda a: ..., {"a": 1}),  # noqa: ARG005
            (lambda **kw: ..., {"a": 1}),  # noqa: ARG005
            (lambda a=1, **kw: ..., {"a": 1}),  # noqa: ARG005
            (lambda a=1, **kw: ..., {"a": 1, "b": 2}),  # noqa: ARG005
        ],
    )
    def test_succeed_kwargs_validation(
        self, fn: Callable[..., None], kwargs: dict[str, Any]
    ) -> None:
        """kwarg validation must fail.

        Args:
            fn (Callable[..., None]): Task function.
            kwargs (dict[str, Any]): Input kwargs.
        """

        sig = inspect.signature(fn)
        validate_kwargs(sig, kwargs)


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
                        name="test",
                        launch_script=Path(),
                        mode="wrap",
                        cmd="sbatch",
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

    @pytest.fixture
    def _set_sdag_base_path(self) -> Generator[None]:
        """Safely set the base path.

        Cache and environment variable are safely cleaned up.
        """
        get_compile_settings.cache_clear()
        os.environ["SDAG_BASE_PATH"] = "data"
        yield
        del os.environ["SDAG_BASE_PATH"]
        get_compile_settings.cache_clear()

    def mock_stage(self) -> None:
        """Mocked stage function."""

    def mock_stage_artifact(self, a: Artifact, b: str) -> None:
        """Mocked stage function with artifacts."""

    def test_add_default_values(self) -> None:
        """Test the default value addition."""
        sdag = MockSDAG()
        task = Task(
            fn=self.mock_stage,
            name="mock_stage",
            mode="wrap",
            cmd="sbatch",
            caching=True,
            retries=2,
            launch_script=Path(),
            register=sdag.register,
            get_uid=sdag.get_uid,
        )

        def fn(a: str, b: int = 5, c: str = "c") -> None:
            """Mocked function with default values."""

        sig = inspect.signature(fn)
        kwargs = {"a": "a", "c": "custom_c"}
        task._add_default_values(sig, kwargs)

        assert kwargs == {"a": "a", "b": 5, "c": "custom_c"}

    def test_add_kwargs(self) -> None:
        """Test the complete kwarg addition."""

        def fn(a: str, b: Artifact, **kw: Any) -> None:
            """Mocked task function."""

        sdag = MockSDAG()
        task = Task(
            fn=fn,
            name="fn",
            mode="wrap",
            cmd="sbatch",
            caching=True,
            retries=2,
            launch_script=Path(),
            register=sdag.register,
            get_uid=sdag.get_uid,
        )

        kwargs = {"a": "a", "b": "/path", "custom_kw": 10}
        node = Node(
            uid="0",
            behavior=TaskNode(
                fname="fn",
                name="fn",
                caching=True,
                mode="wrap",
                cmd="sbatch",
                retries=2,
                launch_script=Path(),
            ),
        )
        task._add_kwargs(node, kwargs)

        assert node.behavior.input_kwargs == [
            InputKwarg(key="a", value="a"),
            InputKwarg(key="b", value="/path"),
            InputKwarg(key="custom_kw", value=10),
        ]

        assert node.output_artifacts == [
            Artifact(name="b", path=Path("/path"))
        ]

    @pytest.mark.parametrize(
        argnames=("original", "expected"),
        argvalues=[
            # Relative path, prepend base path
            ("hello.txt", "data/hello.txt"),
            # Absolute path, do not prepend
            ("/hello.txt", "/hello.txt"),
        ],
    )
    @pytest.mark.usefixtures("_set_sdag_base_path")
    def test_prepend_base_path_when_set(
        self,
        original: str,
        expected: str,
    ) -> None:
        """Test the artifact registration with base path.

        Args:
            original (str): Original path.
            expected (str): Expected artifact path.
        """

        def fn(a: Artifact) -> None:
            """Mocked task with artifact."""

        sdag = MockSDAG()
        task = Task(
            fn=fn,
            name="fn",
            caching=True,
            retries=2,
            launch_script=Path(),
            mode="wrap",
            cmd="sbatch",
            register=sdag.register,
            get_uid=sdag.get_uid,
        )

        kwargs = {"a": original}
        sig = inspect.signature(fn)

        task._prepend_base_path_if_set(kwargs, sig)
        assert kwargs["a"] == expected

    def test_do_not_prepend_base_path(self) -> None:
        """SDAG home is not set, do not prepend."""

        def fn(a: Artifact) -> None:
            """Mocked task with artifact."""

        sdag = MockSDAG()
        task = Task(
            fn=fn,
            name="fn",
            caching=True,
            retries=2,
            launch_script=Path(),
            mode="wrap",
            cmd="sbatch",
            register=sdag.register,
            get_uid=sdag.get_uid,
        )

        original = "hello.txt"
        kwargs = {"a": original}
        sig = inspect.signature(fn)

        task._prepend_base_path_if_set(kwargs, sig)
        assert kwargs["a"] == original

    def test_call(self) -> None:
        """Test the task call."""

        def fn(kwarg: Any, static_input: dict[str, int]) -> None:
            """Mocked task with static and dynamic args."""

        sdag = MockSDAG()
        task = Task(
            fn=fn,
            name="fn",
            caching=True,
            mode="wrap",
            cmd="sbatch",
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
            name="mock_stage_artifact",
            caching=False,
            retries=1,
            mode="wrap",
            cmd="sbatch",
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

    def test_call_with_input(self, mocker: MockerFixture) -> None:
        """Test the pipeline call with input arguments.

        Args:
            mocker (MockerFixture): Mocker fixture.
        """

        def func(a: str) -> None:
            """Mocked pipeline function with input argument."""

        sdag = MockSDAG()
        parent_node = Node(uid="0", behavior=RootNode())
        pipeline = Pipeline(
            fn=func, set_dag=sdag.set_dag, get_graph=sdag.get_graph
        )

        spy = mocker.spy(pipeline, "fn")
        pipeline(parent_node, a="test")
        spy.assert_called_once_with(a="test")

    def test_call_with_output(self) -> None:
        """Test a pipeline not returning the end node."""

        sdag = MockSDAG()

        def func() -> Node[RootNode]:
            """Mocked pipeline returning a node."""
            return Node(uid="0", behavior=RootNode())

        pipeline = Pipeline(
            fn=func, set_dag=sdag.set_dag, get_graph=sdag.get_graph
        )

        output = pipeline()
        assert isinstance(output, Node)

    def test_get_end(self) -> None:
        """Check the end node retrieval.+

        It must contain the artifacts of all nodes in the graph.
        """
        node = Node(
            uid="1",
            behavior=TaskNode(
                fname="fname",
                name="name",
                mode="wrap",
                cmd="sbatch",
                launch_script=Path("submit.sh"),
                caching=True,
                retries=0,
            ),
        )
        node.register_artifact(key="key", path=Path())
        end = Node(uid="2", behavior=EndNode())
        end.add_logical_edge(node.uid)
        graph = Graph(meta=GraphMetadata(name="graph"))
        graph.nodes = [node, end]

        sdag = MockSDAG()
        pipeline = Pipeline(
            fn=lambda: ..., set_dag=sdag.set_dag, get_graph=sdag.get_graph
        )

        endnode = pipeline._get_end(graph)

        assert endnode is end
        assert endnode.artifacts["key"].key == "key"
        assert endnode.artifacts["key"].node is node
        assert endnode.artifacts["key"].path == Path()
