"""Wrappers."""

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from sdag.models import (
    Artifact,
    ArtifactContainer,
    Graph,
    IfNode,
    InputKwarg,
    Node,
    TaskNode,
)


class Task:
    """Object returned by the task decorator.

    Atrributes:
        fn (Callable): Decorated stage.
        caching (bool): If True, stage caching is enabled.
        retries (int): Number of retries.
        launch_script (Path): Submit script path.
        _register (Callable[[Node], None]): Task registration hook.
        _get_uid (Callable[[], str]): Hook to get a unique id.
    """

    def __init__(
        self,
        fn: Callable,
        caching: bool,
        retries: int,
        launch_script: Path,
        register: Callable[[Node], None],
        get_uid: Callable[[], str],
    ):
        """Initialize a task.

        Args:
            fn (Callable): Decorated stage.
            caching (bool): If True, stage caching is enabled.
            retries (int): Number of retries.
            launch_script (Path): Submit script path.
            register (Callable[[Node], None]): Task registration hook.
            get_uid (Callable[[], str]): Hook to get a unique id.
        """
        self.fn = fn
        self.caching = caching
        self.retries = retries
        self.launch_script = launch_script
        self._register = register
        self._get_uid = get_uid

    def __call__(self, *args: Node, **kwargs: Any) -> Node[TaskNode]:
        """Generate a node out of a task.

        This is triggered within pipelines.

        Returns:
            Node[TaskNode]: Node associated to the task.
        """
        uid = self._get_uid()

        node = Node(
            uid=uid,
            behavior=TaskNode(
                fname=self.fn.__name__,
                caching=self.caching,
                retries=self.retries,
                launch_script=self.launch_script,
            ),
        )

        for k, v in self.fn.__annotations__.items():
            if v == Artifact:
                node.register_artifact(k)

        for parent in args:
            node.add_logical_edge(parent.uid)

        for key, value in kwargs.items():
            # Parent node output
            if isinstance(value, Node):
                node.add_output_edge(value.uid, key)

            elif isinstance(value, ArtifactContainer):
                node.add_artifact_edge(
                    parent_uid=value.node.uid, key=key, name=value.key
                )

            else:
                input_kwarg = InputKwarg(key=key, value=value)
                node.behavior.input_kwargs.append(input_kwarg)

        self._register(node)
        return node


class IfWrapper:
    """If node wrapper.

    Attributes:
        node (Node[IfNode]): Node returning the Boolean value.
        push_branch (Callable[..., None]): Hook to push a branch
            to the stack.
    """

    def __init__(self, node: Node[IfNode], push_branch: Callable[..., None]):
        """Initialize the task.

        Args:
            node (Node[IfNode]): Condition node.
            push_branch (Callable[..., None]): Hook to push a branch
                to the stack to mark it as active.
        """
        self.node = node
        self.push_branch = push_branch

    def __enter__(self) -> Self:
        """Enter the positive branch.

        Returns:
            Self: This task.
        """
        self.node.behavior.in_context = True
        self.push_branch(self.node)
        return self

    def __exit__(self, type, value, traceback) -> None:  # noqa: A002
        """Exit the positive branch."""
        self.node.behavior.in_context = False


class Pipeline:
    """Object returned by the pipeline decorator.

    Attributes:
        fn (Callable[[], None]): Decorated pipeline function.
        set_dag (Callable[[str], None]): Hook to set the current
            dag during compilation.
        get_graph (Callable[[], Graph]): Hook to retrieve the
            current graph.
    """

    def __init__(
        self,
        fn: Callable[[], None],
        set_dag: Callable[[str], None],
        get_graph: Callable[[], Graph],
    ):
        """Initialize the pipeline.

        Args:
            fn (Callable[[], None]): Decorated pipeline function.
            set_dag (Callable[[str], None]): Hook to set the current
                dag during compilation.
            get_graph (Callable[[], Graph]): Hook to retrieve the
                current graph.
        """
        self.fn = fn
        self.set_dag = set_dag
        self.get_graph = get_graph

    def __call__(self, *args: Node) -> Node:
        """Run the pipeline function to compile it.

        *args (Node): Parent nodes.

        Returns:
            Node: End node of the pipeline.
        """
        graph = self.compile()
        root = graph.get_root()
        for parent in args:
            root.add_logical_edge(parent.uid)

        return graph.get_end()

    def compile(self) -> Graph:
        """Compile the current pipeline.

        Returns:
            Graph: Pipeline graph.
        """
        self.set_dag(self.fn.__name__)
        self.fn()
        return self.get_graph()
