import json
from pathlib import Path
from typing import Any, Callable, Self, get_type_hints

from sdag.models import Graph, IfNode, InputKwarg, Node, TaskNode


class Task:
    def __init__(
        self,
        fn: Callable,
        caching: bool,
        retries: int,
        launch_script: Path,
        register: Callable[[Self], None],
        get_uid: Callable[[], str],
    ):
        self.fn = fn
        self.caching = caching
        self.retries = retries
        self.launch_script = launch_script
        self._register = register
        self._get_uid = get_uid

    def __call__(self, *args: Node, **kwargs: Any):
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
            if isinstance(value, Node):
                value.add_edge(node, name)
            else:
                serialized_value = json.dumps(value)
                input_kwarg = InputKwarg(key=name, value=serialized_value)
                node.behavior.input_kwargs.append(input_kwarg)

        self._register(node)
        return node

    def _get_return_type(self) -> str | None:
        hints = get_type_hints(self.fn)
        return_type = None
        return_hint = hints.get("return")
        if return_hint is not None and return_hint is not type(None):
            return_type = str(return_hint)

        return return_type


class IfTask:
    def __init__(
        self,
        node: Node[IfNode],
        pop_stack: Callable[..., None],
        push_stack: Callable[..., None],
    ):
        self.node = node
        self.pop_stack = pop_stack
        self.push_stack = push_stack

    def __enter__(self) -> Self:
        self.node.behavior.active = True
        self.push_stack(self.node)
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.node.behavior.active = False
        self.pop_stack()


class Pipeline:
    def __init__(
        self,
        fn: Callable[[], None],
        set_dag: Callable[[str], None],
        get_graph: Callable[[], Graph],
    ):
        self.fn = fn
        self.set_dag = set_dag
        self.get_graph = get_graph

    def __call__(self, **kwargs: Node):
        graph = self.compile()
        root = graph.get_root()
        for parent in kwargs.values():
            parent.add_edge(root)

        return graph.get_end()

    def compile(self) -> Graph:
        self.set_dag(name=self.fn.__name__)
        self.fn()
        return self.get_graph()
