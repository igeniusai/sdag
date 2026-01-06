"""Test compiler optimizations."""

from pathlib import Path

import pytest

from sdag.models import (
    ArtifactType,
    Graph,
    GraphMetadata,
    InputKwarg,
    LogicalType,
    Node,
    NodeUnion,
    OutputType,
    Parent,
    RootNode,
    TaskNode,
)
from sdag.optimizer import GraphJoiner


class TestGraphJoiner:
    """Test cached node joins."""

    @pytest.fixture
    def joiner(self) -> GraphJoiner:
        """Cached node joiner.

        Returns:
            GraphJoiner: Joiner.
        """
        return GraphJoiner()

    @pytest.mark.parametrize(
        argnames=("node", "res"),
        argvalues=[
            # Not a task
            (
                Node(uid="2", behavior=RootNode()),
                False,
            ),
            # Not cached
            (
                Node(
                    uid="0",
                    behavior=TaskNode(
                        fname="foo",
                        launch_script=Path("script.sh"),
                        caching=False,
                        retries=0,
                    ),
                ),
                False,
            ),
            # Dynamic input
            (
                Node(
                    uid="0",
                    parents=[Parent(uid="1", parent_type=OutputType(key="k"))],
                    behavior=TaskNode(
                        fname="foo",
                        launch_script=Path("script.sh"),
                        caching=True,
                        retries=0,
                    ),
                ),
                False,
            ),
            (
                Node(
                    uid="0",
                    parents=[Parent(uid="1", parent_type=LogicalType())],
                    behavior=TaskNode(
                        fname="foo",
                        launch_script=Path("script.sh"),
                        caching=True,
                        retries=0,
                    ),
                ),
                True,
            ),
        ],
    )
    def test_is_node_eligible(
        self, node: NodeUnion, res: bool, joiner: GraphJoiner
    ) -> None:
        """Test node eligibility to be fused.

        Args:
            node (NodeUnion): Node checked.
            res (bool): Eligibility result.
            joiner (GraphJoiner): Joiner.
        """
        assert joiner._is_node_eligible(node) == res

    def test_replace_parent_uids(self, joiner: GraphJoiner) -> None:
        """Test the parent uid replacement after join.

        Args:
            joiner (GraphJoiner): Joiner.
        """
        node = Node(
            uid="0",
            parents=[
                Parent(uid="1", parent_type=LogicalType()),
                Parent(uid="2", parent_type=OutputType(key="k")),
            ],
            behavior=TaskNode(
                fname="foo",
                launch_script=Path("script.sh"),
                caching=True,
                retries=0,
            ),
        )
        swaps = {"2": "3"}
        joiner._replace_parent_uids(node, swaps)

        assert node.parents[0].uid == "1"
        assert node.parents[1].uid == "3"

    @pytest.mark.parametrize(
        argnames=("node_parents", "swap_parents", "res"),
        argvalues=[
            # Empty
            ([], [], []),
            # Transfer dependency
            (
                [Parent(uid="2", parent_type=LogicalType())],
                [],
                [Parent(uid="2", parent_type=LogicalType())],
            ),
            # Do not duplicate edges
            (
                [Parent(uid="2", parent_type=LogicalType())],
                [Parent(uid="2", parent_type=LogicalType())],
                [Parent(uid="2", parent_type=LogicalType())],
            ),
            # Add new edge
            (
                [Parent(uid="2", parent_type=LogicalType())],
                [Parent(uid="3", parent_type=LogicalType())],
                [
                    Parent(uid="3", parent_type=LogicalType()),
                    Parent(uid="2", parent_type=LogicalType()),
                ],
            ),
            # Some parent but different edges
            (
                [
                    Parent(
                        uid="2",
                        parent_type=ArtifactType(
                            key="a", name="b", path=Path()
                        ),
                    )
                ],
                [Parent(uid="2", parent_type=LogicalType())],
                [
                    Parent(uid="2", parent_type=LogicalType()),
                    Parent(
                        uid="2",
                        parent_type=ArtifactType(
                            key="a", name="b", path=Path()
                        ),
                    ),
                ],
            ),
        ],
    )
    def test_merge_nodes(
        self,
        node_parents: list[Parent],
        swap_parents: list[Parent],
        res: list[Parent],
        joiner: GraphJoiner,
    ) -> None:
        """Test node merges.

        Args:
            node_parents (list[Parent]): Merged node parents.
            swap_parents (list[Parent]): Surviving node parents.
            res (list[Parent]): Surviving node parents after join.
            joiner (GraphJoiner): Joiner.
        """
        node = Node(
            uid="0",
            parents=node_parents,
            behavior=TaskNode(
                fname="foo",
                launch_script=Path("script.sh"),
                caching=True,
                retries=0,
            ),
        )
        swap = Node(
            uid="1",
            parents=swap_parents,
            behavior=TaskNode(
                fname="foo",
                launch_script=Path("script.sh"),
                caching=True,
                retries=0,
            ),
        )

        joiner._merge_nodes(node, swap)
        assert swap.parents == res

    @pytest.mark.parametrize(
        argnames=("node", "swap", "res"),
        argvalues=[
            # Different function name
            (
                Node(
                    uid="0",
                    behavior=TaskNode(
                        fname="a",
                        launch_script=Path(),
                        caching=True,
                        retries=0,
                    ),
                ),
                Node(
                    uid="1",
                    behavior=TaskNode(
                        fname="b",
                        launch_script=Path(),
                        caching=True,
                        retries=0,
                    ),
                ),
                False,
            ),
            # Node depends on swap
            (
                Node(
                    uid="0",
                    parents=[Parent(uid="1", parent_type=LogicalType())],
                    behavior=TaskNode(
                        fname="a",
                        launch_script=Path(),
                        caching=True,
                        retries=0,
                    ),
                ),
                Node(
                    uid="1",
                    behavior=TaskNode(
                        fname="a",
                        launch_script=Path(),
                        caching=True,
                        retries=0,
                    ),
                ),
                False,
            ),
            # Swap depends on node
            (
                Node(
                    uid="0",
                    behavior=TaskNode(
                        fname="a",
                        launch_script=Path(),
                        caching=True,
                        retries=0,
                    ),
                ),
                Node(
                    uid="1",
                    parents=[Parent(uid="0", parent_type=LogicalType())],
                    behavior=TaskNode(
                        fname="a",
                        launch_script=Path(),
                        caching=True,
                        retries=0,
                    ),
                ),
                False,
            ),
            # Input values are different
            (
                Node(
                    uid="0",
                    behavior=TaskNode(
                        fname="a",
                        launch_script=Path(),
                        caching=True,
                        retries=0,
                        input_kwargs=[InputKwarg(key="d", value={"a": 1})],
                    ),
                ),
                Node(
                    uid="1",
                    behavior=TaskNode(
                        fname="a",
                        launch_script=Path(),
                        caching=True,
                        retries=0,
                    ),
                ),
                False,
            ),
            # Everything is ok
            (
                Node(
                    uid="0",
                    behavior=TaskNode(
                        fname="a",
                        launch_script=Path(),
                        caching=True,
                        retries=0,
                        input_kwargs=[InputKwarg(key="d", value={"a": 1})],
                    ),
                ),
                Node(
                    uid="1",
                    behavior=TaskNode(
                        fname="a",
                        launch_script=Path(),
                        caching=True,
                        retries=0,
                        input_kwargs=[InputKwarg(key="d", value={"a": 1})],
                    ),
                ),
                True,
            ),
        ],
    )
    def test_match(
        self,
        node: Node[TaskNode],
        swap: Node[TaskNode],
        res: bool,
        joiner: GraphJoiner,
    ) -> None:
        """Test node and swap matches.

        Args:
            node (Node[TaskNode]): Node that could be replaced by swap.
            swap (Node[TaskNode]): Surviving node.
            res (bool): True if nodes can be swapped.
            joiner (GraphJoiner): Joiner.
        """
        assert joiner._match(node, swap) == res

    def test_find_swaps(self, joiner: GraphJoiner) -> None:
        """Test the swap identification.

        Args:
            joiner (GraphJoiner): Joiner.
        """
        nodes: list[NodeUnion] = [
            Node(
                uid="0",
                behavior=TaskNode(
                    fname="foo",
                    launch_script=Path("script.sh"),
                    caching=True,
                    retries=0,
                    input_kwargs=[InputKwarg(key="key", value="value")],
                ),
            ),
            Node(
                uid="1",
                behavior=TaskNode(
                    fname="foo",
                    launch_script=Path("script.sh"),
                    caching=True,
                    retries=0,
                    input_kwargs=[InputKwarg(key="key", value="value")],
                ),
            ),
            Node(
                uid="2",
                behavior=TaskNode(
                    fname="foo",
                    launch_script=Path("script.sh"),
                    caching=True,
                    retries=0,
                    input_kwargs=[
                        InputKwarg(key="key", value="another value")
                    ],
                ),
            ),
        ]

        assert joiner._find_swaps(nodes) == {"1": "0"}

    def test_fuse_nodes(self, joiner: GraphJoiner) -> None:
        """Test node fusion.

        Args:
            joiner (GraphJoiner): Joiner.
        """
        nodes: list[NodeUnion] = [
            Node(
                uid="0",
                behavior=TaskNode(
                    fname="foo",
                    launch_script=Path("script.sh"),
                    caching=True,
                    retries=0,
                    input_kwargs=[InputKwarg(key="key", value="value")],
                ),
            ),
            Node(
                uid="1",
                behavior=TaskNode(
                    fname="foo",
                    launch_script=Path("script.sh"),
                    caching=True,
                    retries=0,
                    input_kwargs=[InputKwarg(key="key", value="value")],
                ),
            ),
            Node(
                uid="2",
                parents=[Parent(uid="1", parent_type=LogicalType())],
                behavior=TaskNode(
                    fname="foo",
                    launch_script=Path("script.sh"),
                    caching=True,
                    retries=0,
                    input_kwargs=[
                        InputKwarg(key="key", value="another value")
                    ],
                ),
            ),
        ]

        fused_nodes = joiner._fuse_nodes(nodes, swaps={"1": "0"})
        assert fused_nodes == [
            Node(
                uid="0",
                behavior=TaskNode(
                    fname="foo",
                    launch_script=Path("script.sh"),
                    caching=True,
                    retries=0,
                    input_kwargs=[InputKwarg(key="key", value="value")],
                ),
            ),
            Node(
                uid="2",
                parents=[Parent(uid="0", parent_type=LogicalType())],
                behavior=TaskNode(
                    fname="foo",
                    launch_script=Path("script.sh"),
                    caching=True,
                    retries=0,
                    input_kwargs=[
                        InputKwarg(key="key", value="another value")
                    ],
                ),
            ),
        ]

    def test_join_nodes(self, joiner: GraphJoiner) -> None:
        """Test the complete joining process.

        Args:
            joiner (GraphJoiner): Joiner.
        """
        graph = Graph(
            meta=GraphMetadata(name="pipeline"),
            nodes=[
                Node(
                    uid="0",
                    behavior=TaskNode(
                        fname="foo",
                        launch_script=Path("script.sh"),
                        caching=True,
                        retries=0,
                        input_kwargs=[InputKwarg(key="key", value="value")],
                    ),
                ),
                Node(
                    uid="1",
                    parents=[Parent(uid="3", parent_type=LogicalType())],
                    behavior=TaskNode(
                        fname="foo",
                        launch_script=Path("script.sh"),
                        caching=True,
                        retries=0,
                        input_kwargs=[InputKwarg(key="key", value="value")],
                    ),
                ),
                Node(
                    uid="2",
                    parents=[Parent(uid="1", parent_type=LogicalType())],
                    behavior=RootNode(),
                ),
            ],
        )

        joiner.join_nodes(graph)
        assert graph.nodes == [
            Node(
                uid="0",
                parents=[Parent(uid="3", parent_type=LogicalType())],
                behavior=TaskNode(
                    fname="foo",
                    launch_script=Path("script.sh"),
                    caching=True,
                    retries=0,
                    input_kwargs=[InputKwarg(key="key", value="value")],
                ),
            ),
            Node(
                uid="2",
                parents=[Parent(uid="0", parent_type=LogicalType())],
                behavior=RootNode(),
            ),
        ]
