"""Node I/O management."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings

from sdag.exceptions import NodeNotFoundError, NotATaskError
from sdag.models import Graph, Node, Parent, TaskNode


class Settings(BaseSettings):
    """Env variables set by the scheduler.

    Attributes:
        sdag_pipeline (str): Pipeline name.
        sdag_uid (str): Node unique id.
        sdag_try_num: Number of times the task has been executed.
    """

    sdag_pipeline: Path
    sdag_uid: str
    sdag_try_num: int


class IOManager:
    """Manage I/O.

    Attributes:
        settings (Settings): Scheduler environment variables.
        _pipeline_fname: DAG filename.
        output.json: Node output filename.
    """

    def __init__(self):
        """Initialize the i/o manager."""
        self.settings = Settings()  # type: ignore
        self._pipeline_fname = "pipeline.json"
        self._output_fname = "output.json"

    def get_input(self, node: Node[TaskNode], fn: Callable) -> dict[str, Any]:
        """Get the task input values.

        Parent args are read from their output file. Static input
        kwargs are taken from the DAG file.

        Args:
            node (Node[TaskNode]): Node to be executed.
            fn (Callable): Task function.

        Returns:
            dict[str, Any]: Node input kwargs.
        """
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

    def serialize_output(self, output: Any) -> None:
        """Serialize the output as JSON.

        Args:
            output (Any): Node output.
            uid (str): Node unique id.
        """
        data = {"output": output}
        uid = self.settings.sdag_uid
        p = self.settings.sdag_pipeline / uid / self._output_fname
        with p.open("w") as f:
            json.dump(data, f)

    def find_node_to_be_executed(self) -> Node[TaskNode]:
        """Find the task node in the DAG JSON file.

        Raises:
            NotATaskError: The node is not a task.

        Returns:
            Node[TaskNode]: Node that is going to be executed.
        """
        dag = self._read_dag()
        node = self._find_this_node(dag)
        if not isinstance(node.behavior, TaskNode):
            raise NotATaskError(uid=node.uid)

        return node

    def _read_dag(self) -> Graph:
        """Parse the compiled pipeline.

        Returns:
            Graph: Parsed DAG.
        """
        p = self.settings.sdag_pipeline
        with (p / self._pipeline_fname).open() as f:
            data = json.load(f)
        return Graph.model_validate(data)

    def _read_parent_output(self, parent: Parent) -> Any:
        """Read the input kwargs coming from parents.

        Args:
            parent (Parent): Parent of the node to be executed.

        Returns:
            Any: Parent output.
        """
        p = self.settings.sdag_pipeline / parent.uid
        with (p / self._output_fname).open() as f:
            data = json.load(f)
        return data["output"]

    def _find_this_node(self, dag: Graph) -> Node:
        """Find the node to be executed within the DAG.

        Args:
            dag (Graph): Parsed DAG.

        Raises:
            NodeNotFoundError: The node is not found in the DAG.

        Returns:
            Node: Node to be executed.
        """
        for node in dag.nodes:
            if self.settings.sdag_uid == node.uid:
                return node

        raise NodeNotFoundError(uid=self.settings.sdag_uid)
