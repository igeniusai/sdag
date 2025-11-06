"""Node I/O management."""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from pydantic_settings import BaseSettings

from sdag.exceptions import (
    ArtifactNotFoundError,
    NodeNotFoundError,
    NotATaskError,
)
from sdag.models import (
    Artifact,
    ArtifactType,
    Graph,
    Node,
    OutputType,
    Parent,
    TaskNode,
    TaskOutput,
)


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
            if parent.parent_type.type == "Output":
                parent = cast(Parent[OutputType], parent)
                key = parent.parent_type.key
                if key in argnames:
                    parent_output = self._read_parent_output(parent)
                    input_kwargs[key] = parent_output

            if parent.parent_type.type == "Artifact":
                parent = cast(Parent[ArtifactType], parent)
                parent_artifact = self._read_parent_artifact(parent)
                input_kwargs[parent.parent_type.key] = parent_artifact

        for kwarg in node.behavior.input_kwargs:
            value = json.loads(kwarg.value)
            input_kwargs[kwarg.key] = value

        return input_kwargs

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

    def _read_dag(self) -> Graph:
        """Parse the compiled pipeline.

        Returns:
            Graph: Parsed DAG.
        """
        p = self.settings.sdag_pipeline
        with (p / self._pipeline_fname).open() as f:
            data = json.load(f)
        return Graph.model_validate(data)

    def _read_parent_output(self, parent: Parent[OutputType]) -> Any:
        """Read the input kwargs coming from parents.

        Args:
            parent (Parent[OutputType]): Parent of the node to
                be executed.

        Returns:
            Any: Parent output.
        """
        p = self.settings.sdag_pipeline / parent.uid
        with (p / self._output_fname).open() as f:
            data = f.read()

        output = TaskOutput.model_validate_json(data)
        return output.output

    def _read_parent_artifact(self, parent: Parent[ArtifactType]) -> Path:
        """Read parent artifact.

        Args:
            parent (Parent[ArtifactType]): Parent.

        Raises:
            ArtifactNotFoundError: The artifact is not found.

        Returns:
            Path: Artifact path.
        """
        p = self.settings.sdag_pipeline / parent.uid
        with (p / self._output_fname).open() as f:
            data = f.read()

        output = TaskOutput.model_validate_json(data)
        for artifact in output.artifacts:
            if artifact.name == parent.parent_type.name:
                return Path(artifact.path)

        raise ArtifactNotFoundError(
            parent_uid=parent.uid, artifact_name=parent.parent_type.name
        )

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
