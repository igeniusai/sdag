"""Node I/O management."""

import inspect
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings

from sdag.exceptions import NodeNotFoundError, NotATaskError
from sdag.models import (
    Artifact,
    Graph,
    Node,
    TaskNode,
    TaskOutput,
)
from sdag.wrappers import validate_kwargs


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
        _output_fname: Node output filename as specified in the
            scheduler.
        _input_fname: Node output filename as specified in the
            scheduler.
    """

    def __init__(self):
        """Initialize the i/o manager."""
        self.settings = Settings()  # type: ignore
        self._pipeline_fname = "pipeline.json"
        self._output_fname = "output.json"
        self._input_fname = "input.json"

    def get_input(self, fn: Callable) -> dict[str, Any]:
        """Read the input data.

        It's written by the scheduler in the node directory.

        Args:
            fn (Callable): Task function. Use to verify the
                input data.

        Returns:
            dict[str, Any]: Node input kwargs.
        """
        uid = self.settings.sdag_uid
        p = self.settings.sdag_pipeline / uid / self._input_fname
        with p.open() as f:
            input_data: dict[str, Any] = json.load(f)

        self._validate_input_data(input_data, fn)
        return input_data

    def get_artifacts(
        self, fn: Callable, input_kwargs: dict[str, Any]
    ) -> dict[str, Artifact]:
        """Build artifacts for execution.

        Args:
            fn (Callable): Task to be called.
            input_kwargs (dict[str, Any]): Input data. Those
                corresponding to artifacts will be turned into
                paths.

        Returns:
            dict[str, Artifact]: Artifact keys and artifacts.
                They will be merged with the input_kwargs.
        """
        artifacts: dict[str, Artifact] = {}
        for k, v in fn.__annotations__.items():
            if v == Artifact:
                path = input_kwargs.get(k, "")
                artifacts[k] = Artifact(name=k, path=Path(path))
        return artifacts

    def serialize_output(
        self, output: Any, artifacts: dict[str, Artifact]
    ) -> None:
        """Serialize the output as JSON.

        Args:
            output (Any): Node output.
            artifacts (dict[str, Artifact]): Output artifacts.
        """
        artifact_list = list(artifacts.values())
        task_output = TaskOutput(output=output, artifacts=artifact_list)
        data = task_output.model_dump_json()

        uid = self.settings.sdag_uid
        p = self.settings.sdag_pipeline / uid / self._output_fname
        with p.open("w") as f:
            f.write(data)

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

    def _validate_input_data(
        self, input_data: dict[str, Any], fn: Callable
    ) -> None:
        """Validate the input data.

        Args:
            input_data (dict[str, Any]): Input data.
            fn (Callable): Task function associated to the data.
        """
        sig = inspect.signature(fn)
        validate_kwargs(sig, input_data)

    def _read_dag(self) -> Graph:
        """Parse the compiled pipeline.

        Returns:
            Graph: Parsed DAG.
        """
        p = self.settings.sdag_pipeline
        with (p / self._pipeline_fname).open() as f:
            data = json.load(f)
        return Graph.model_validate(data)

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
