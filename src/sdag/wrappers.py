"""Wrappers."""

import inspect
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from sdag.exceptions import DynamicArtifactError, KwargNotFoundError
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
        fn: Callable[..., Any],
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

        for parent in args:
            node.add_logical_edge(parent.uid)

        self._add_kwargs(node, kwargs)
        self._register(node)

        return node

    def _add_kwargs(
        self, node: Node[TaskNode], kwargs: dict[str, Any]
    ) -> None:
        """Add kwargs.

        Args:
            node (Node[TaskNode]): Task node.
            kwargs (dict[str, Any]): Input kwargs.

        Raises:
            KwargNotFoundError: Keyword argument not found in
                the input values.
        """
        sig = inspect.signature(self.fn)
        self._add_default_values(sig, kwargs)
        self._validate_kwargs(sig, kwargs)

        for key, val in kwargs.items():
            param = sig.parameters.get(key)
            if param is not None and param.annotation == Artifact:
                self._set_output_artifact(node, key, val)

            self._handle_input_kwarg(node, key, val)

    def _add_default_values(
        self, sig: inspect.Signature, kwargs: dict[str, Any]
    ) -> None:
        """Add default values to missing kwargs.

        Args:
            sig (inspect.Signature): Function signature.
            kwargs (dict[str, Any]): Input kwargs.
        """
        for key, param in sig.parameters.items():
            if key not in kwargs and param.default is not param.empty:
                kwargs[key] = param.default

    def _validate_kwargs(
        self, sig: inspect.Signature, kwargs: dict[str, Any]
    ) -> None:
        """Validate kwarg consistency.

        Args:
            sig (inspect.Signature): Function signature.
            kwargs (dict[str, Any]): Input kwargs.

        Raises:
            KwargNotFoundError: Keyword argument not found in
                the input values.
        """
        any_var = any(p.kind == p.VAR_KEYWORD for p in sig.parameters.values())
        for key in kwargs:
            if key not in sig.parameters and not any_var:
                raise KwargNotFoundError(key)

        for key, param in sig.parameters.items():
            if key not in kwargs and not param.kind == param.VAR_KEYWORD:
                raise KwargNotFoundError(key)

    def _set_output_artifact(
        self, node: Node[TaskNode], key: str, value: Any
    ) -> None:
        """Set output artifact.

        Args:
            node (Node[TaskNode]): Task node.
            key (str): Artifact key.
            value (Any): Input value.

        Raises:
            DynamicArtifactError: Dynamic artifacts are not supported.
        """
        if isinstance(value, Node):
            raise DynamicArtifactError(key)

        path = (
            value.path if isinstance(value, ArtifactContainer) else Path(value)
        )
        node.register_artifact(key, path)

    def _handle_input_kwarg(
        self, node: Node[TaskNode], key: str, value: Any
    ) -> None:
        """Handle input kwargs.

        Three types of input kwargs are possible:
        - dynamic input
        - artifacts
        - static input

        Args:
            node (Node[TaskNode]): Child node.
            key (str): Input key.
            value (Any): Input value.
        """
        if isinstance(value, Node):
            node.add_output_edge(value.uid, key)

        elif isinstance(value, ArtifactContainer):
            node.add_artifact_edge(
                parent_uid=value.node.uid,
                key=key,
                name=value.key,
                path=value.path,
            )

        else:
            input_kwarg = InputKwarg(key=key, value=value)
            node.behavior.input_kwargs.append(input_kwarg)


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
        fn: Callable[..., Any],
        set_dag: Callable[[str], None],
        get_graph: Callable[[], Graph],
    ):
        """Initialize the pipeline.

        Args:
            fn (Callable[..., None]): Decorated pipeline function.
            set_dag (Callable[[str], None]): Hook to set the current
                dag during compilation.
            get_graph (Callable[[], Graph]): Hook to retrieve the
                current graph.
        """
        self.fn = fn
        self.set_dag = set_dag
        self.get_graph = get_graph

    def __call__(self, *args: Node, **input_kwargs: Any) -> Node:
        """Call the pipeline function to compile it.

        args (Node): Parent nodes.
        input_kwargs (Any): Pipeline input kwargs.

        Returns:
            Node: End node of the pipeline.
        """
        self.set_dag(self.fn.__name__)
        output = self.fn(**input_kwargs)
        graph = self.get_graph()
        self._mark_nodes_requiring_output(graph)

        root = graph.get_root()
        for parent in args:
            root.add_logical_edge(parent.uid)

        return output if output is not None else graph.get_end()

    def compile(self, input_kwargs: dict[str, Any] | None = None) -> Graph:
        """Compile the current pipeline.

        Args:
            input_kwargs (dict[str, Any] | None): Pipeline input
                kwargs. Defaults to None.

        Returns:
            Graph: Pipeline graph.
        """
        input_kwargs = {} if input_kwargs is None else input_kwargs
        self.set_dag(self.fn.__name__)
        self.fn(**input_kwargs)
        graph = self.get_graph()
        self._mark_nodes_requiring_output(graph)
        return graph

    def _mark_nodes_requiring_output(self, graph: Graph) -> None:
        """Nodes requiring output are tagged.

        Args:
            graph (Graph): Compiled graph.
        """
        uids = set()
        for node in graph.nodes:
            uids.update(
                p.uid for p in node.parents if p.parent_type.type == "Output"
            )

        self._bubbles_up_oneof(uids, graph)

    def _bubbles_up_oneof(self, uids: set[str], graph: Graph) -> None:
        """Assign output used flag and propagate OneOf nodes.

        Args:
            uids (set[str]): Nodes that require the output.
            graph (Graph): Compiled graph.
        """
        nodemap = {node.uid: node for node in graph.nodes}
        while uids:
            node = nodemap[uids.pop()]
            node.output_used = True
            if node.behavior.type == "OneOfNode":
                uids.update(p.uid for p in node.parents)
