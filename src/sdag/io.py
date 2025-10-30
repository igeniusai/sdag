import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings

from sdag.models import Graph, Node, Parent, TaskNode


class Settings(BaseSettings):
    sdag_pipeline: Path
    sdag_uid: str


class IOManager:
    def __init__(self):
        self.settings = Settings()
        self._pipeline_fname = "pipeline.json"
        self._output_fname = "output.json"

    def get_input(self, node: Node[TaskNode], fn: Callable) -> Any:
        input_kwargs: dict[str, Any] = {}
        argnames = fn.__code__.co_varnames
        for parent in node.parents:
            if parent.name and parent.name in argnames:
                parent_output = self._read_parent_output(parent)
                input_kwargs[parent.name] = parent_output

        for kwarg in node.behavior.input_kwargs:
            value = json.loads(kwarg.value)
            input_kwargs[kwarg.key] = value

        return input_kwargs

    def serialize_output(self, output: Any, uid: str) -> None:
        data = {"output": output}
        p = self.settings.sdag_pipeline / uid / self._output_fname
        with p.open("w") as f:
            json.dump(data, f)

    def find_node_to_be_executed(self) -> Node[TaskNode]:
        dag = self._read_dag()
        node = self._find_this_node(dag)
        if not isinstance(node.behavior, TaskNode):
            msg = f"Node {node.uid} is not a task!"
            raise ValueError(msg)
        return node

    def _read_dag(self) -> Graph:
        p = self.settings.sdag_pipeline
        with (p / self._pipeline_fname).open() as f:
            data = json.load(f)
        return Graph.model_validate(data)

    def _read_parent_output(self, parent: Parent) -> None:
        p = self.settings.sdag_pipeline / parent.uid
        with (p / self._output_fname).open() as f:
            data = json.load(f)
        return data["output"]

    def _find_this_node(self, dag: Graph) -> Node:
        for node in dag.nodes:
            if self.settings.sdag_uid == node.uid:
                return node
        msg = f"Node {self.settings.sdag_uid} not found"
        raise ValueError(msg)
