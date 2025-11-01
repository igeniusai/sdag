"""Tasks and pipeline."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, Self, get_type_hints

from sdag.models import Graph, IfNode, InputKwarg, Node, TaskNode


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
        return_type = self._get_return_type()

        node = Node(
            uid=uid,
            behavior=TaskNode(
                type="TaskNode",
                fname=self.fn.__name__,
                caching=self.caching,
                retries=self.retries,
                launch_script=self.launch_script,
                return_type=return_type,
            ),
        )

        for parent in args:
            parent.add_edge(node)

        for name, value in kwargs.items():
            # Parent node output
            if isinstance(value, Node):
                value.add_edge(node, name)
            else:
                # Static input kwargs
                serialized_value = json.dumps(value)
                input_kwarg = InputKwarg(key=name, value=serialized_value)
                node.behavior.input_kwargs.append(input_kwarg)

        self._register(node)
        return node

    def _get_return_type(self) -> str | None:
        """Get the return type of the task.

        Not used at the moment.

        Returns:
            str | None: Return type.
        """
        hints = get_type_hints(self.fn)
        return_type = None
        return_hint = hints.get("return")
        if return_hint is not None and return_hint is not type(None):
            return_type = str(return_hint)

        return return_type


class IfTask:
    """Task associated with If/Elif branches.

    Attributes:
        node (Node[IfNode]): Node returning the Boolean value.
        pop_stack (Callable[..., None]): Hook to pop a branch
            from the stack. Used for nested if/elif.
        push_stack (Callable[..., None]): Hook to push a branch
            to the stack. Used for nested if/elif.
    """

    def __init__(
        self,
        node: Node[IfNode],
        pop_stack: Callable[..., None],
        push_stack: Callable[..., None],
    ):
        """Initialize the task.

        Args:
            node (Node[IfNode]): Condition node.
            pop_stack (Callable[..., None]): Hook to pop a branch
                from the stack. Used for nested if/elif.
            push_stack (Callable[..., None]): Hook to push a branch
                to the stack. Used for nested if/elif.
        """
        self.node = node
        self.pop_stack = pop_stack
        self.push_stack = push_stack

    def __enter__(self) -> Self:
        """Enter the positive branch.

        Returns:
            Self: This task.
        """
        self.node.behavior.active = True
        self.push_stack(self.node)
        return self

    def __exit__(self, type, value, traceback) -> None:  # noqa: A002
        """Exit the positive branch."""
        self.node.behavior.active = False
        self.pop_stack()


class Pipeline:
    """Object returned by the pipeline decorator.

    Not properly a task but it has the same function.

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
            parent.add_edge(root)

        return graph.get_end()

    def compile(self) -> Graph:
        """Compile the current pipeline.

        Returns:
            Graph: Pipeline graph.
        """
        self.set_dag(self.fn.__name__)
        self.fn()
        return self.get_graph()
