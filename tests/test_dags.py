"""DAG tests."""

import json
from pathlib import Path

import pytest

from sdag.dags import DAG, SDAG
from sdag.exceptions import (
    DAGNotSetError,
    IncorrectElifError,
    MissingActiveBranchError,
    TaskNotUniqueError,
)
from sdag.models import (
    BranchType,
    Graph,
    GraphMetadata,
    IfNode,
    LogicalType,
    Node,
    Parent,
    RootNode,
    TaskNode,
)
from sdag.wrappers import IfWrapper, Pipeline


def test_sdag_obj_is_reachable():
    """sdag can be imported."""
    from sdag.sdag import sdag

    assert isinstance(sdag, SDAG)


class TestDAG:
    """Test DAG class."""

    @pytest.fixture
    def dag(self) -> DAG:
        """dag.

        Returns:
            DAG: dag.
        """
        return DAG(uid="0", name="dag")

    def test_register_no_branch(self, dag: DAG) -> None:
        """Node registration without active branch.

        Args:
            dag (DAG): dag
        """
        node = Node(uid="1", behavior=RootNode())
        dag.register(node)
        assert dag.graph.nodes == [node]

    def test_push_branch(self, dag: DAG) -> None:
        """Push a branch to the stack.

        Args:
            dag (DAG): dag.
        """
        node = Node(uid="1", behavior=IfNode(branch=True))
        dag.push_stack(node)
        assert dag.branchstack == [node]

    def test_join(self, dag: DAG) -> None:
        """Join two graphs.

        Args:
            dag (DAG): dag.
        """
        node1 = Node(uid="1", behavior=RootNode())
        node2 = Node(uid="2", behavior=RootNode())
        dag.register(node1)
        graph = Graph(meta=GraphMetadata(name="graph"), nodes=[node2])
        dag.join(graph)

        assert sorted(dag.graph.nodes, key=lambda x: x.uid) == [node1, node2]

    def test_add_root(self, dag: DAG) -> None:
        """Root addition to the graph.

        Args:
            dag (DAG): dag.
        """
        node1 = Node(uid="1", behavior=RootNode())
        node2 = Node(
            uid="2",
            parents=[Parent(uid="1", parent_type=LogicalType())],
            behavior=RootNode(),
        )
        dag.register(node1)
        dag.register(node2)

        dag.add_root()
        assert any(node.uid == "_dag_0_root_" for node in dag.graph.nodes)
        assert node1.parents == [
            Parent(uid="_dag_0_root_", parent_type=LogicalType())
        ]

    def test_add_end(self, dag: DAG) -> None:
        """End node addition to the graph.

        Args:
            dag (DAG): dag.
        """
        node1 = Node(uid="1", behavior=RootNode())
        node2 = Node(
            uid="2",
            parents=[Parent(uid="1", parent_type=LogicalType())],
            behavior=RootNode(),
        )
        dag.register(node1)
        dag.register(node2)

        dag.add_end()
        end = next(
            node for node in dag.graph.nodes if node.uid == "_dag_0_end_"
        )
        assert end.parents == [Parent(uid="2", parent_type=LogicalType())]

    def test_get_empty_graph(self, dag: DAG) -> None:
        """Only root and end are present.

        Args:
            dag (DAG): dag.
        """
        graph = dag.get_graph()
        assert len(graph.nodes) == 2

    def test_register_within_branch(self, dag: DAG) -> None:
        """Register a node within a branch.

        Args:
            dag (DAG): dag.
        """
        branch = Node(uid="1", behavior=IfNode(in_context=True))
        dag.push_stack(branch)

        node = Node(uid="2", behavior=RootNode())
        dag.register(node)

        assert node.parents == [
            Parent(uid="1", parent_type=BranchType(branch=True))
        ]

    def test_register_after_branch_exit(self, dag: DAG) -> None:
        """register a node after a branch exited.

        Args:
            dag (DAG): dag.
        """
        branch = Node(uid="1", behavior=IfNode(in_context=False))
        dag.push_stack(branch)

        node = Node(uid="2", behavior=RootNode())
        dag.register(node)

        assert not node.parents
        assert dag.branchstack == [branch]
        assert branch.behavior.to_be_dropped

    def test_register_and_drop_branch(self, dag: DAG) -> None:
        """Register a node and drop the branch.

        Args:
            dag (DAG): dag.
        """
        branch = Node(
            uid="1", behavior=IfNode(in_context=False, to_be_dropped=True)
        )
        dag.push_stack(branch)

        node = Node(uid="2", behavior=RootNode())
        dag.register(node)

        assert not node.parents
        assert not dag.branchstack

    def test_elif_registration(self, dag: DAG) -> None:
        """Full elif condition registration.

        Args:
            dag (DAG): dag.
        """
        branch = Node(
            uid="1", behavior=IfNode(in_context=False, to_be_dropped=True)
        )
        dag.push_stack(branch)

        node = Node(
            uid="2",
            behavior=TaskNode(
                fname="fname",
                launch_script=Path("script.sh"),
                caching=False,
                retries=0,
            ),
        )
        dag.register_elif_expression(node)
        assert node.parents == [
            Parent(uid="1", parent_type=BranchType(branch=False))
        ]

    def test_elif_registration_no_branch(self, dag: DAG) -> None:
        """elif called within another branch.

        Args:
            dag (DAG): dag.
        """
        node = Node(
            uid="2",
            behavior=TaskNode(
                fname="fname",
                launch_script=Path("script.sh"),
                caching=False,
                retries=0,
            ),
        )
        with pytest.raises(MissingActiveBranchError):
            dag.register_elif_expression(node)

    def test_elif_registration_in_context(self, dag: DAG) -> None:
        """Register elif in context.

        Args:
            dag (DAG): dag.
        """
        branch = Node(
            uid="1", behavior=IfNode(in_context=True, to_be_dropped=True)
        )
        dag.push_stack(branch)

        node = Node(
            uid="2",
            behavior=TaskNode(
                fname="fname",
                launch_script=Path("script.sh"),
                caching=False,
                retries=0,
            ),
        )
        with pytest.raises(IncorrectElifError):
            dag.register_elif_expression(node)

    def test_elif_registration_no_drop(self, dag: DAG) -> None:
        """The previous branch is not fully exited.

        Args:
            dag (DAG): dag.
        """
        branch = Node(
            uid="1", behavior=IfNode(in_context=False, to_be_dropped=False)
        )
        dag.push_stack(branch)

        node = Node(
            uid="2",
            behavior=TaskNode(
                fname="fname",
                launch_script=Path("script.sh"),
                caching=False,
                retries=0,
            ),
        )
        with pytest.raises(IncorrectElifError):
            dag.register_elif_expression(node)


class TestSDAG:
    """SDAG tests."""

    @pytest.fixture
    def sdag(self) -> SDAG:
        """sdag.

        Returns:
            SDAG: sdag object.
        """
        return SDAG()

    def test_get_uid(self, sdag: SDAG) -> None:
        """Test the UID retrieval.

        Args:
            sdag (SDAG): sdag.
        """
        uid = sdag.get_uid()
        assert isinstance(uid, str)
        assert int(uid) < sdag.uid

    def test_reset_uid(self, sdag: SDAG) -> None:
        """Test uid deterministic behavior.

        Args:
            sdag (SDAG): sdag.
        """
        uid1 = sdag.get_uid()
        sdag._reset_uid()
        uid2 = sdag.get_uid()
        assert uid1 == uid2

    def test_task_decoration(self, sdag: SDAG) -> None:
        """Test the task decorator.

        Args:
            sdag (SDAG): sdag.
        """

        @sdag.task(launch_script="submit.sh")
        def foo(): ...

        assert sdag.taskdict == {"foo": foo.fn}

    def test_duplicate_task_decoration(self, sdag: SDAG) -> None:
        """Duplicate task names are not allowed.

        Args:
            sdag (SDAG): sdag.
        """

        @sdag.task(launch_script="submit.sh")
        def foo(): ...  # type: ignore

        with pytest.raises(TaskNotUniqueError):

            @sdag.task(launch_script="submit.sh")
            def foo(): ...

    def test_pipeline_deconration(self, sdag: SDAG) -> None:
        """Test the pipeline decorator.

        Args:
            sdag (SDAG): sdag.
        """

        @sdag.pipeline
        def pipeline(): ...

        assert isinstance(pipeline, Pipeline)

    def test_first_dag_set(self, sdag: SDAG) -> None:
        """Test the dag set without other dags.

        Args:
            sdag (SDAG): sdag.
        """
        sdag.set_current_dag("dag")
        assert isinstance(sdag.current, DAG)

    def test_second_dag_set(self, sdag: SDAG) -> None:
        """Test the dag set with another active dag.

        Args:
            sdag (SDAG): sdag.
        """
        sdag.set_current_dag("dag1")
        sdag.set_current_dag("dag2")
        assert isinstance(sdag.current, DAG)
        assert isinstance(sdag.dagstack[0], DAG)

    def test_push_branch_without_dag(self, sdag: SDAG) -> None:
        """DAG does not exist.

        Args:
            sdag (SDAG): sdag.
        """
        branch = Node(uid="1", behavior=IfNode())
        with pytest.raises(DAGNotSetError):
            sdag.push_branch(branch)

    def test_push_branch(self, sdag: SDAG) -> None:
        """Push a branch to the stack.

        Args:
            sdag (SDAG): sdag.
        """
        sdag.set_current_dag(name="dag")
        branch = Node(uid="1", behavior=IfNode())
        sdag.push_branch(branch)
        assert sdag.current is not None
        assert sdag.current.branchstack == [branch]

    def test_register_without_dag(self, sdag: SDAG) -> None:
        """Test reistration without a DAG.

        Args:
            sdag (SDAG): sdag.
        """
        node = Node(uid="1", behavior=RootNode())
        with pytest.raises(DAGNotSetError):
            sdag.register(node)

    def test_register(self, sdag: SDAG) -> None:
        """Register a node.

        Args:
            sdag (SDAG): sdag.
        """
        sdag.set_current_dag(name="dag")
        node = Node(uid="1", behavior=RootNode())
        sdag.register(node)

        assert sdag.current is not None
        assert sdag.current.graph.nodes == [node]

    def test_oneof(self, sdag: SDAG) -> None:
        """Test the OneOf node.

        Args:
            sdag (SDAG): sdag.
        """
        sdag.set_current_dag(name="dag")
        node = Node(uid="1", behavior=RootNode())
        oneof = sdag.OneOf(node)

        assert sdag.current is not None
        assert sdag.current.graph.nodes == [oneof]
        assert oneof.parents == [Parent(uid="1", parent_type=LogicalType())]

    def test_oneof_without_dag(self, sdag: SDAG) -> None:
        """DAG does not exist.

        Args:
            sdag (SDAG): sdag.
        """
        node = Node(uid="1", behavior=RootNode())
        with pytest.raises(DAGNotSetError):
            sdag.OneOf(node)

    def test_get_one_graph(self, sdag: SDAG) -> None:
        """Test the retrieval of the only graph.

        Args:
            sdag (SDAG): sdag.
        """
        sdag.set_current_dag("dag")
        graph = sdag.get_graph()
        assert isinstance(graph, Graph)
        assert sdag.current is None

    def test_get_two_graphs(self, sdag: SDAG) -> None:
        """Test the retrieval in the case of two graphs.

        Args:
            sdag (SDAG): sdag.
        """
        sdag.set_current_dag("dag1")
        sdag.set_current_dag("dag2")
        graph = sdag.get_graph()
        assert isinstance(graph, Graph)
        assert isinstance(sdag.current, DAG)

    def test_get_missing_graph(self, sdag: SDAG) -> None:
        """Try to retrieve a graph the doesnt' exist.

        Args:
            sdag (SDAG): sdag.
        """
        with pytest.raises(DAGNotSetError):
            sdag.get_graph()

    def test_register_if(self, sdag: SDAG) -> None:
        """Test the if branch registration.

        Args:
            sdag (SDAG): sdag.
        """
        sdag.set_current_dag(name="dag")
        node = Node(
            uid="1",
            behavior=TaskNode(
                fname="fname",
                launch_script=Path("submit.sh"),
                caching=False,
                retries=0,
            ),
        )
        wrapper = sdag.If(node)

        assert sdag.current is not None
        assert sdag.current.graph.nodes == [wrapper.node]
        assert wrapper.node.parents == [
            Parent(uid="1", parent_type=LogicalType())
        ]

    def test_if_without_dag(self, sdag: SDAG) -> None:
        """Register an If to a non-existing dag.

        Args:
            sdag (SDAG): sdag.
        """
        node = Node(
            uid="1",
            behavior=TaskNode(
                fname="fname",
                launch_script=Path("submit.sh"),
                caching=False,
                retries=0,
            ),
        )
        with pytest.raises(DAGNotSetError):
            sdag.If(node)

    def test_elif_without_dag(self, sdag: SDAG) -> None:
        """Register an elif to a non-existing dag.

        Args:
            sdag (SDAG): sdag.
        """
        node = Node(
            uid="1",
            behavior=TaskNode(
                fname="fname",
                launch_script=Path("submit.sh"),
                caching=False,
                retries=0,
            ),
        )
        with pytest.raises(DAGNotSetError):
            sdag.Elif(node)

    def test_register_elif(self, sdag: SDAG) -> None:
        """Test the elif registration.

        Args:
            sdag (SDAG): sdag.
        """
        sdag.set_current_dag(name="dag")

        @sdag.task(launch_script="submit.sh")
        def if_cond(): ...

        @sdag.task(launch_script="submit.sh")
        def elif_cond(): ...

        with sdag.If(if_cond()):
            ...

        wrapper = sdag.Elif(elif_cond())
        assert isinstance(wrapper, IfWrapper)

    def test_compile(self, sdag: SDAG, tmp_path: Path) -> None:
        """Empty pipeline compilation

        There are only root and end nodes.
        """

        @sdag.pipeline
        def pipeline(): ...

        json_path = tmp_path / "pipeline.json"
        sdag.compile(pipeline, json_path)

        with json_path.open() as f:
            data = json.load(f)

        graph = Graph.model_validate(data)

        assert graph.meta.name == "pipeline"
        assert len(graph.nodes) == 2
