"""DAG tests."""

from pathlib import Path

import pytest

from sdag.dags import DAG, SDAG
from sdag.exceptions import IncorrectElifError, MissingActiveBranchError
from sdag.models import (
    BranchType,
    Graph,
    IfNode,
    LogicalType,
    Node,
    Parent,
    RootNode,
    TaskNode,
)


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
        graph = Graph(name="graph", nodes=[node2])
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
