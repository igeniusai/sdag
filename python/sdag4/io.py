"""Task I/O management."""

import inspect
import json
from pathlib import Path
from typing import Any

from sdag4.commands import get_task
from sdag4.compiler import master
from sdag4.discovery import find_pipeline_by_name
from sdag4.models import ArtifactEdge, TaskOutput
from sdag4.wrappers import Task, is_artifact, validate_kwargs


def find_and_import_task(
    task_name: str, subpipeline_name: str, import_path: str
) -> Task:
    """Sufficiently clever way to import tasks.

    Args:
        task_name (str): Task name.
        subpipeline_name (str): Subpipeline name.
        import_path (str): Pipeline import path.

    Returns:
        Task: Imported task.
    """
    if subpipeline_name in master.pipelines:
        pipeline = master.pipelines[subpipeline_name]
        if task_name in pipeline.tasks:
            return pipeline.tasks[task_name]
    if task_name in master.tasks:
        return master.tasks[task_name]

    find_pipeline_by_name(import_path)
    return get_task(task_name, subpipeline_name)


class IOHandler:
    """Manage I/O.

    Attributes:
        uid (int): Task uid.
        _pipeline_fname: DAG filename.
        _output_fname: Node output filename as specified in the
            scheduler.
        _input_fname: Node output filename as specified in the
            scheduler.
    """

    def __init__(self, uid: int, pipeline_dir: Path):
        """Initialize the I/O handler.

        Args:
            uid (int): Task unique UID.
            pipeline_dir (Path): Pipeline working directory.
        """
        self.uid = uid
        self.pipeline_dir = pipeline_dir
        self._pipeline_fname = "pipeline.json"
        self._output_fname = "output.json"
        self._input_fname = "input.json"

    def get_input(self, sig: inspect.Signature) -> dict[str, Any]:
        """Read the input data.

        It's written by the scheduler in the node directory.

        Args:
            sig (inspect.Signature): Task signature used to
                validate the input values.

        Returns:
            dict[str, Any]: Task input kwargs.
        """
        input_path = self.pipeline_dir / f"{self.uid}" / self._input_fname
        with input_path.open() as f:
            input_data: dict[str, Any] = json.load(f)

        validate_kwargs(sig, input_data)
        return input_data

    def get_artifacts(
        self, sig: inspect.Signature, input_kwargs: dict[str, Any]
    ) -> dict[str, ArtifactEdge]:
        """Build artifacts for execution.

        Args:
            sig (inspect.Signature): Task function signature.
            input_kwargs (dict[str, Any]): Input data. Those
                corresponding to artifacts will be turned into
                paths.

        Returns:
            dict[str, ArtifactEdge]: Artifact keys and artifacts.
                They will be merged with the input_kwargs.
        """
        artifacts: dict[str, ArtifactEdge] = {}
        for key, param in sig.parameters.items():
            if is_artifact(param):
                path = input_kwargs.get(key, "")
                artifacts[key] = ArtifactEdge(name=key, path=Path(path))
        return artifacts

    def serialize_output(
        self, output: Any, artifacts: dict[str, ArtifactEdge]
    ) -> None:
        """Serialize the output as JSON.

        Args:
            output (Any): Node output.
            artifacts (dict[str, ArtifactEdge]): Output artifacts.
        """
        artifact_list = list(artifacts.values())
        task_output = TaskOutput(output=output, artifacts=artifact_list)
        data = task_output.model_dump_json()
        output_path = self.pipeline_dir / f"{self.uid}" / self._output_fname

        with output_path.open("w") as f:
            f.write(data)

    def cast_values(
        self, sig: inspect.Signature, input_kwargs: dict[str, Any]
    ) -> None:
        """Automatically cast to Path if required.

        Args:
            sig (inspect.Signature): Task signature.
            input_kwargs (dict[str, Any]): Input arguments.
        """
        for key, param in sig.parameters.items():
            if param.annotation is Path or (
                is_artifact(param) and param.annotation.__origin__ is Path
            ):
                input_kwargs[key] = Path(input_kwargs[key])

    def serialize_artifacts(self, artifacts: dict[str, ArtifactEdge]) -> str:
        """Serialize artifacts as string for logging.

        Args:
            artifacts (dict[str, ArtifactEdge]): Artifacts.

        Returns:
            str: Serialized artifacts.
        """
        if not artifacts:
            return "{}"

        return "\n".join(
            artifact.model_dump_json(indent=4)
            for artifact in artifacts.values()
        )
