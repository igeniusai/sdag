"""Test task I/O."""

import json
import os
from pathlib import Path
from typing import Any

import pytest

from sdag.exceptions import (
    KwargNotFoundError,
    NodeNotFoundError,
    NotATaskError,
)
from sdag.io import IOManager
from sdag.models import Graph


class TestIOManager:
    """Test the I/O manager."""

    @pytest.fixture
    def io_manager(self, tmp_path: Path) -> IOManager:
        """I/O manager.

        Args:
            tmp_path (Path): Temporary path fixture.

        Returns:
            IOManager: I/O manager.
        """
        os.environ["SDAG_PIPELINE"] = str(tmp_path)
        os.environ["SDAG_UID"] = "0"
        os.environ["SDAG_TRY_NUM"] = "1"

        return IOManager()

    @pytest.fixture
    def graph_dict(self) -> dict[str, Any]:
        """Graph dictionary.

        Returns:
            dict[str, Any]: Graph dictionary.
        """
        return {
            "meta": {
                "name": "test",
                "creation_dt": "1920-01-01 09:20:20",
            },
            "nodes": [
                {
                    "uid": "0",
                    "behavior": {
                        "type": "TaskNode",
                        "fname": "func",
                        "launch_script": "/",
                        "caching": False,
                        "retries": 0,
                    },
                    "parents": [
                        {
                            "uid": "1",
                            "parent_type": {
                                "type": "Output",
                                "key": "input_data",
                            },
                        },
                    ],
                },
                {
                    "uid": "1",
                    "behavior": {"type": "RootNode", "children": ["0"]},
                },
            ],
        }

    @pytest.mark.parametrize(
        argnames="output",
        argvalues=[None, True, "test", 1, [1, 2, 3], {"a": 1}],
    )
    def test_output_serialization(
        self, output: Any, io_manager: IOManager
    ) -> None:
        """Check the output serialization in JSON.

        Args:
            output (Any): Output.
            io_manager (IOManager): I/O manager.
        """
        settings = io_manager.settings
        path = settings.sdag_pipeline / settings.sdag_uid
        path.mkdir(parents=True, exist_ok=True)

        io_manager.serialize_output(output, artifacts={})

        with (path / io_manager._output_fname).open() as f:
            data = json.load(f)

        assert data == {"output": output, "artifacts": []}

    def test_read_dag(
        self, io_manager: IOManager, graph_dict: dict[str, Any]
    ) -> None:
        """Test the DAG deserialization.

        Args:
            io_manager (IOManager): I/O manager.
            graph_dict (dict[str, Any]): graph to be read.
        """
        pipeline_dir = io_manager.settings.sdag_pipeline
        pipeline_dir.mkdir(parents=True, exist_ok=True)
        path = pipeline_dir / io_manager._pipeline_fname
        with path.open("w") as f:
            json.dump(graph_dict, f)

        graph = io_manager._read_dag()
        assert isinstance(graph, Graph)

    def test_find_this_node(
        self, io_manager: IOManager, graph_dict: dict[str, Any]
    ) -> None:
        """Check the node identification in the graph.

        Args:
            io_manager (IOManager): I/O manager.
            graph_dict (dict[str, Any]): Graph dictionary.
        """
        graph = Graph.model_validate(graph_dict)
        node = io_manager._find_this_node(graph)
        assert node.uid == "0"

    def test_node_not_found(
        self, io_manager: IOManager, graph_dict: dict[str, Any]
    ) -> None:
        """Check the exception raise if the node is not found.

        Args:
            io_manager (IOManager): I/O manager.
            graph_dict (dict[str, Any]): Graph dictionary.
        """
        io_manager.settings.sdag_uid = "-1"
        graph = Graph.model_validate(graph_dict)
        with pytest.raises(NodeNotFoundError):
            io_manager._find_this_node(graph)

    def test_find_node_to_be_executed(
        self, io_manager: IOManager, graph_dict: dict[str, Any]
    ) -> None:
        """Test the node identification.

        Args:
            io_manager (IOManager): I/O manager.
            graph_dict (dict[str, Any]): Graph dictionary.
        """
        pipeline_dir = io_manager.settings.sdag_pipeline
        pipeline_dir.mkdir(parents=True, exist_ok=True)
        path = pipeline_dir / io_manager._pipeline_fname
        with path.open("w") as f:
            json.dump(graph_dict, f)

        node = io_manager.find_node_to_be_executed()
        assert node.uid == "0"

    def test_node_to_be_executed_is_not_a_task(
        self, io_manager: IOManager, graph_dict: dict[str, Any]
    ) -> None:
        """Check the node nature.

        Only tasks can be executed, other nodes must raise
        an exception.

        Args:
            io_manager (IOManager): I/O manager.
            graph_dict (dict[str, Any]): Graph dictionary.
        """
        io_manager.settings.sdag_uid = "1"
        pipeline_dir = io_manager.settings.sdag_pipeline
        pipeline_dir.mkdir(parents=True, exist_ok=True)
        path = pipeline_dir / io_manager._pipeline_fname
        with path.open("w") as f:
            json.dump(graph_dict, f)

        with pytest.raises(NotATaskError):
            io_manager.find_node_to_be_executed()

    def test_get_input(self, io_manager: IOManager) -> None:
        """Test the input value retrieval.

        Args:
            io_manager (IOManager): I/O manager.
        """

        def foo(a: Any) -> None: ...

        pipeline_dir = io_manager.settings.sdag_pipeline
        uid = io_manager.settings.sdag_uid
        path = pipeline_dir / uid
        path.mkdir(parents=True, exist_ok=True)
        input_data = {"a": True}
        with (path / io_manager._input_fname).open("w") as f:
            json.dump(input_data, f)

        input_retrieved = io_manager.get_input(fn=foo)
        assert input_data == input_retrieved

    def test_input_not_in_signature(self, io_manager: IOManager) -> None:
        """The input key is not in the function signature.

        Args:
            io_manager (IOManager): I/O manager.
        """

        def foo() -> None: ...

        pipeline_dir = io_manager.settings.sdag_pipeline
        uid = io_manager.settings.sdag_uid
        path = pipeline_dir / uid
        path.mkdir(parents=True, exist_ok=True)
        input_data = {"a": True}
        with (path / io_manager._input_fname).open("w") as f:
            json.dump(input_data, f)

        with pytest.raises(KwargNotFoundError):
            io_manager.get_input(fn=foo)

    def test_input_kwargs(self, io_manager: IOManager) -> None:
        """Test the input kwargs.

        The input key is not in the function signature but the function
        contains input kwargs, no error is thrown.

        Args:
            io_manager (IOManager): I/O manager.
        """

        def foo(**kwargs) -> None: ...

        pipeline_dir = io_manager.settings.sdag_pipeline
        uid = io_manager.settings.sdag_uid
        path = pipeline_dir / uid
        path.mkdir(parents=True, exist_ok=True)
        input_data = {"a": True}
        with (path / io_manager._input_fname).open("w") as f:
            json.dump(input_data, f)

        io_manager.get_input(fn=foo)
