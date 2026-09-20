"""Wrappers."""

import inspect
import logging
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from sdag.compiler import compiler, master
from sdag.exceptions import (
    DynamicArtifactError,
    IncorrectElifError,
    IncorrectElseError,
    KwargNotFoundError,
    TaskNotUniqueError,
)
from sdag.models import (
    DAG,
    ArtifactContainer,
    ArtifactSig,
    BaseNode,
    BranchNode,
    EndNode,
    Kwarg,
    NodeUnion,
    OneOfNode,
    OutputParent,
    Parent,
    RootNode,
    ScriptContent,
    ScriptPath,
    ScriptUnion,
    TaskNode,
)
from sdag.settings import get_compile_settings
from sdag.types import Commands, ExecMode, Scope

logger = logging.getLogger(__name__)


def is_artifact(param: inspect.Parameter | None) -> bool:
    """Check if a type hint is actually an artifact.

    Args:
        param (inspect.Parameter | None): Parameter.

    Returns:
        bool: True if it's an artifact.
    """
    if param is None:
        return False
    annotation = param.annotation
    if not hasattr(annotation, "__metadata__"):
        return False
    meta = annotation.__metadata__
    return meta and meta[0] == ArtifactSig


def validate_kwargs(sig: inspect.Signature, kwargs: dict[str, Any]) -> None:
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


def oneof(*args: NodeUnion, name: str = "one_of") -> OneOfNode:
    """OneOf node.

    It has two major use cases:
    1. Selecting nodes from mutually excluded branches.
    2. Selecting The first successfully completed parent.

    Returns:
        Node[OneOfNode]: OneOf node.
    """
    node = OneOfNode(uid=compiler.get_uid(), name=name)
    compiler.register(node)
    for parent in args:
        node.add_logical_edge(parent.uid)

    # TODO artifacts
    return node


class If:
    """Branch wrapper.

    Attributes:
        branch (BranchNode): Branch.
    """

    def __init__(self, expr: TaskNode | OneOfNode):
        """Initialize the branch wrapper.

        Args:
            expr (TaskNode | OneOfNode): Task returning a bool output.
        """
        parent = Parent(uid=expr.uid, kind=OutputParent(key="expr"))
        self.branch = BranchNode(uid=compiler.get_uid(), parents=[parent])

    def __enter__(self) -> Self:
        """Enter the positive branch.

        Returns:
            Self: This task.
        """
        compiler.register(self.branch)
        compiler.push_scope((self.branch.uid, True))
        return self

    def __exit__(self, type, value, traceback) -> None:  # noqa: A002
        """Exit the positive branch."""
        compiler.pop_scope()
        compiler.close_clause(self.branch.uid, next_edge_value=False)


class Elif:
    """Elif Wrapper.

    Must follow an upstream If or Elif clause in the same scope, with
    nothing but plain task calls in between.

    Attributes:
        _expr (TaskNode | OneOfNode): Task returning a bool output.
        branch (BranchNode): Branch.
    """

    def __init__(self, expr: TaskNode | OneOfNode):
        """Initialize the Elif wrapper.

        Args:
            expr (TaskNode | OneOfNode): Task returning a bool output.

        Raises:
            IncorrectElifError: Not immediately preceded by an If or
                Elif clause in the same scope.
        """
        prev = compiler.current_scope.last_clause
        if prev is None or prev.terminal:
            raise IncorrectElifError

        expr.add_branch_edge(prev.branch_uid, prev.next_edge_value)

        parent = Parent(uid=expr.uid, kind=OutputParent(key="expr"))
        self.branch = BranchNode(uid=compiler.get_uid(), parents=[parent])

    def __enter__(self) -> Self:
        """Enter the positive branch.

        Returns:
            Self: This task.
        """
        compiler.register(self.branch)
        compiler.push_scope((self.branch.uid, True))
        return self

    def __exit__(self, type, value, traceback) -> None:  # noqa: A002
        """Exit the positive branch."""
        compiler.pop_scope()
        compiler.close_clause(self.branch.uid, next_edge_value=False)


class Else:
    """Else wrapper.

    Must follow an upstream If or Elif clause in the same scope, with
    nothing but plain task calls in between.
    """

    def __enter__(self) -> Self:
        """Enter the else context.

        Raises:
            IncorrectElseError: Not immediately preceded by an If or
                Elif clause in the same scope.

        Returns:
            Self: Else.
        """
        prev = compiler.current_scope.last_clause
        if prev is None or prev.terminal:
            raise IncorrectElseError

        self._branch_uid = prev.branch_uid
        compiler.push_scope((prev.branch_uid, prev.next_edge_value))
        return self

    def __exit__(self, exc_type, exc, tb):
        """Exit the context."""
        compiler.pop_scope()
        compiler.close_clause(
            self._branch_uid, next_edge_value=False, terminal=True
        )


class Task:
    """Task wrapper.

    Attributes:
        fn (Callable[..., Any]): Task function.
        name (str): Task name.
        cmd (Commands): Task command.
        mode (ExecMode): Task mode.
        scope: (Scope): Task scope.
            Use 'global' for tasks decorated with the
            @task decorator, 'local' for tasks decorated
            with @pipeline.task.
        cache (bool): Enable caching.
        cache_ignore (list[str]): list of fields ignored
            during cache validation.
        cache_size (int): Cache size. Ignore if caching is disabled.
            set to 0 to allow for infinite cache size.
        retries (int): Number of retries.
        script (ScriptUnion): Submission script.
    """

    def __init__(
        self,
        fn: Callable[..., Any],
        name: str,
        cmd: Commands,
        mode: ExecMode,
        scope: Scope,
        cache: bool,
        cache_ignore: list[str] | None,
        cache_size: int,
        retries: int,
        script: ScriptUnion,
        tags: list[str],
    ):
        """Initialize the task wrapper.

        Args:
            fn (Callable[..., Any]): Task function.
            name (str): Task name.
            cmd (Commands): Task command.
            mode (ExecMode): Task mode.
            scope: (Scope): Task scope.
                Use 'global' for tasks decorated with the
                @task decorator, 'local' for tasks decorated
                with @pipeline.task.
            cache (bool): Enable caching.
            cache_ignore (list[str] | None): list of fields
                ignored during cache validation.
            cache_size (int): Cache size. Ignore if caching is
                disabled. set to 0 to allow for infinite cache
                size.
            retries (int): Number of retries.
            script (ScriptUnion): Submission script.
            tags (list[str]): Tags.
        """
        self.fn = fn
        self.name = name
        self.cmd: Commands = cmd
        self.mode: ExecMode = mode
        self.scope: Scope = scope
        self.cache = cache
        self.cache_ignore = cache_ignore if cache_ignore is not None else []
        self.cache_size = cache_size
        self.retries = retries
        self.script = script
        self.tags = tags

    def __call__(self, *args: NodeUnion, **kwargs: Any) -> TaskNode:
        """Call the task to get a node in the graph.

        Args:
            args (NodeUnion): Logical dependencies.
            kwargs (Any): Kwargs, outputs, and artifacts.

        Returns:
            TaskNode: Task node.
        """
        node = TaskNode(
            uid=compiler.get_uid(),
            fn_name=self.fn.__name__,
            name=self.name,
            scope=self.scope,
            cache=self.cache,
            cache_ignore=self.cache_ignore,
            cache_size=self.cache_size,
            mode=self.mode,
            cmd=self.cmd,
            retries=self.retries,
            script=self.script,
            tags=self.tags,
        )

        for parent in args:
            node.add_logical_edge(parent.uid)

        self._add_kwargs(node, kwargs)
        compiler.register(node)

        return node

    def _add_kwargs(self, node: TaskNode, kwargs: dict[str, Any]) -> None:
        """Add kwargs.

        Args:
            node (TaskNode): Task node.
            kwargs (dict[str, Any]): Input kwargs.

        Raises:
            KwargNotFoundError: Keyword argument not found in
                the input values.
        """
        sig = inspect.signature(self.fn)
        self._add_default_values(sig, kwargs)
        validate_kwargs(sig, kwargs)
        self._prepend_base_path_if_set(kwargs, sig)

        for key, val in kwargs.items():
            param = sig.parameters.get(key)
            if is_artifact(param):
                self._set_output_artifact(node, key, val)

            val = kwargs[key]
            self._handle_input_kwarg(node, key, val)

    def _prepend_base_path_if_set(
        self, kwargs: dict[str, Any], sig: inspect.Signature
    ) -> None:
        """Prepend the base path if required.

        If the `SDAG_BASE_PATH` environment variable is set and
        path is not absolute, the registered path is appended to
        the base path.

        Args:
            kwargs (dict[str, Any]): Input kwargs.
            sig (inspect.Signature): Task signature.
        """
        settings = get_compile_settings()
        if settings.sdag_base_path is None:
            return

        logger.debug("sdag base path: '%s'", settings.sdag_base_path)

        for key in kwargs:
            param = sig.parameters.get(key)
            if is_artifact(param):
                path = Path(kwargs[key])
                if not path.is_absolute():
                    kwargs[key] = str(settings.sdag_base_path / path)

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

    def _set_output_artifact(
        self, node: TaskNode, key: str, value: Any
    ) -> None:
        """Set output artifact.

        Args:
            node (TaskNode): Task node.
            key (str): Artifact key.
            value (Any): Input value.

        Raises:
            DynamicArtifactError: Dynamic artifacts are not supported.
        """
        if isinstance(value, NodeUnion.__args__[0]):  # type: ignore
            raise DynamicArtifactError(key)

        path = (
            value.path if isinstance(value, ArtifactContainer) else Path(value)
        )
        node.register_artifact(key, path)

    def _handle_input_kwarg(
        self, node: TaskNode, key: str, value: Any
    ) -> None:
        """Handle input kwargs.

        Three types of input kwargs are possible:
        - dynamic input
        - artifacts
        - static input

        Args:
            node (TaskNode): Child node.
            key (str): Input key.
            value (Any): Input value.
        """
        if isinstance(value, BaseNode):
            node.add_output_edge(value.uid, key)

        elif isinstance(value, ArtifactContainer):
            node.add_artifact_edge(
                parent_uid=value.node.uid,
                key=key,
                name=value.key,
                path=value.path,
            )

        else:
            input_kwarg = Kwarg(key=key, value=value)
            node.add_kwarg(input_kwarg)


class Script:
    """Script content.

    It can be used to embed the script content
    in the pipeline definition.

    Attributes:
        content (str): Script content.
    """

    def __init__(self, content: str):
        """Initialize the Script.

        Args:
            content (str): Script content.
        """
        self.content = content


class Pipeline:
    """Pipeline.

    Attributes:
        fn (Callable[..., Any]): Pipeline function.
        tasks: dict[str, Task]: Pipeline local tasks.
    """

    def __init__(self, fn: Callable[..., Any]):
        """Initialize the pipeline wrapper.

        Args:
            fn (Callable[..., Any]): Pipeline function.
        """
        self.fn = fn
        self.tasks: dict[str, Task] = {}

    def add_task(self, task: Task) -> None:
        """Add a local task.

        Args:
            task (Task): Task local to the pipeline.

        Raises:
            TaskNotUniqueError: A task with the same name
                already exists.
        """
        if task.fn.__name__ in self.tasks:
            raise TaskNotUniqueError(name=task.fn.__name__)
        self.tasks[task.fn.__name__] = task

    def task(
        self,
        script: str | Path | Script,
        name: str | None = None,
        cmd: Commands = "sbatch",
        mode: ExecMode = "wrap",
        cache: bool = False,  # noqa: FBT002
        cache_ignore: list[str] | None = None,
        cache_size: int = 1,
        retries: int = 0,
        tags: list[str] | None = None,
    ) -> Callable[[Callable], Task]:
        """Local task decorator.

        Args:
            script (str | Path | Script): Launch script. It can be
                either or a sdag.Script instance.
            name (str | None, optional): Task name. If null, it will
                default to the function name. Defaults to None.
            cmd (Commands, optional): Command to
                execute the script. Defaults to "sbatch".
            mode (ExecMode, optional): Use wrap to call the
                Python function or ext to call an external script.
                Defaults to "wrap".
            cache (bool, optional): Enable local caching.
                Defaults to False.
            cache_ignore (list[str] | None): List of fields ignored
                during cache validation.
            cache_size (int): Cache size. Ignore if caching is
                disabled. set to 0 to allow for infinite cache
                size.
            retries: (int): Number of retries. Defaults to 0.
            tags (list[str] | None): Task tags, they can be used to
                configure sets of tasks globally.

        Returns:
            Callable[[Callable], Task]: Task wrapper.
        """

        def return_task(fn: Callable[..., Any]) -> Task:
            """Get the task.

            Args:
                fn (Callable[..., Any]): Task function.

            Returns:
                Task: Task.
            """
            task_name = name if name is not None else fn.__name__
            if isinstance(script, Script):
                script_obj = ScriptContent(content=script.content)
            else:
                script_obj = ScriptPath(path=Path(script))

            task = Task(
                fn=fn,
                name=task_name,
                scope="local",
                cmd=cmd,
                mode=mode,
                cache=cache,
                cache_ignore=cache_ignore,
                cache_size=cache_size,
                retries=retries,
                script=script_obj,
                tags=tags if tags is not None else [],
            )
            self.add_task(task)

            return task

        return return_task

    def __call__(self, *args: NodeUnion, **input_kwargs: Any) -> EndNode:
        """Call the pipeline function to compile it.

        Args:
            args (Node): Parent nodes.
            input_kwargs (Any): Pipeline input kwargs.

        Returns:
            Node: End node of the pipeline.
        """
        root = RootNode(uid=compiler.get_uid(), pipeline_name=self.fn.__name__)
        for parent in args:
            root.add_logical_edge(parent.uid)

        compiler.set_dag(root)
        self.fn(**input_kwargs)
        dag, root, end = compiler.get_dag()

        for node in dag.nodes:
            if node.kind in ("one_of", "task"):
                end.join_artifacts(node)  # type: ignore

        return end

    def compile(self, import_path: str | None = None, **kwargs: Any) -> DAG:
        """Call the pipeline function to compile it.

        Args:
            import_path (str | None): Pipeline import path. If null, it will
                be set to the pipeline function name. Defaults to None.
            kwargs (Any): Pipeline input kwargs.

        Returns:
            Node: End node of the pipeline.
        """
        compiler.reset()
        root = RootNode(uid=compiler.get_uid(), pipeline_name=self.fn.__name__)
        compiler.set_dag(root, import_path)
        self.fn(**kwargs)
        dag, _, _ = compiler.get_dag()

        return dag


def task(
    script: str | Path | Script,
    name: str | None = None,
    cmd: Commands = "sbatch",
    mode: ExecMode = "wrap",
    cache: bool = False,  # noqa: FBT002
    cache_ignore: list[str] | None = None,
    cache_size: int = 1,
    retries: int = 0,
    tags: list[str] | None = None,
) -> Callable[[Callable], Task]:
    """Local task decorator.

    Args:
        script (str | Path | Script): Launch script. It can be
            either or a sdag.Script instance.
        name (str | None, optional): Task name. If null, it will
            default to the function name. Defaults to None.
        cmd (Commands, optional): Command to
            execute the script. Defaults to "sbatch".
        mode (ExecMode, optional): Use wrap to call the
            Python function or ext to call an external script.
            Defaults to "wrap".
        cache (bool, optional): Enable caching. Defaults
            to False.
        cache_ignore (list[str] | None): List of fields ignored
            during cache validation. Defaults to None.
        cache_size (int): Cache size. Ignore if caching is
            disabled. set to 0 to allow for infinite cache
            size.
        retries: (int): Number of retries. Defaults to 0.
        tags (list[str] | None): Task tags, they can be used to
            configure sets of tasks globally.

    Returns:
        Callable[[Callable], Task]: Task wrapper.
    """

    def return_task(fn: Callable[..., Any]) -> Task:
        task_name = name if name is not None else fn.__name__
        if isinstance(script, Script):
            script_obj = ScriptContent(content=script.content)
        else:
            script_obj = ScriptPath(path=Path(script))

        task = Task(
            fn=fn,
            name=task_name,
            scope="global",
            cmd=cmd,
            mode=mode,
            cache=cache,
            cache_ignore=cache_ignore,
            cache_size=cache_size,
            retries=retries,
            script=script_obj,
            tags=tags if tags is not None else [],
        )
        master.add_task(task)

        return task

    return return_task


def pipeline(fn: Callable[..., Any]) -> Pipeline:
    """Pipeline decorator.

    Args:
        fn (Callable[..., Any]): Pipeline function.

    Raises:
        TaskNotUniqueError: Another pipeline with
            the same name has already bee imported.

    Returns:
        Pipeline: Pipeline.
    """
    if fn.__name__ in master.pipelines:
        raise TaskNotUniqueError(name=fn.__name__)

    pipeline_obj = Pipeline(fn=fn)
    master.add_pipeline(pipeline_obj)

    return Pipeline(fn=fn)
