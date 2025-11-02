"""DAG model."""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Annotated, Generic, Literal, TypeVar

from pydantic import BaseModel, Field

from sdag.exceptions import EndNotFoundError, RootNotFoundError

StrPath = str | Path


class BaseNodeType(ABC, BaseModel):
    """Base class for all nodes."""

    @abstractmethod
    def add_child(self, uid: str) -> None:
        """Add a child to the current node.

        Args:
            uid (str): Child unique id.
        """

    @abstractmethod
    def is_leaf(self) -> bool:
        """Check wether a node is a leaf.

        Returns:
            bool: True if the node is a leaf, False otherwise.
        """


class InputKwarg(BaseModel):
    """Task node static input kwargs.

    They are copied as JSON strings in the pipeline JSON.

    Attributes:
        key (str): Input name.
        value (str): Input value (JSON string).
    """

    key: str
    value: str


class TaskNode(BaseNodeType):
    """Node associated to a task.

    Tasks are decorated with @SDAG.task. They
    represent the functions that will be executed.

    Attributes:
        type (Literal['TaskNode']): Node type.
        fname (str): Function associated to the task.
        launch_script (Path): Lauch script path.
        caching (bool): Set to True to enable output caching.
        retries (int): Number of retries.
        return_type (str): Return type (not used at the moment).
        input_kwargs (list[InputKwarg]): Static input kwargs.
        children (list[str]): Children uids.
    """

    type: Literal["TaskNode"] = "TaskNode"
    fname: str
    launch_script: Path
    caching: bool
    retries: int
    return_type: None | str = None
    input_kwargs: list[InputKwarg] = Field(default_factory=list)
    children: list[str] = Field(default_factory=list)

    def add_child(self, uid: str) -> None:
        """Add a child.

        Args:
            uid (str): Child uid.
        """
        self.children.append(uid)

    def is_leaf(self) -> bool:
        """Check if the task is a leaf or not.

        Returns:
            bool: True if the task is a leaf.
        """
        return not self.children


class IfNode(BaseNodeType):
    """Node associated to a conditional.

    Attributes:
        type (Literal['IfNode']): Node type.
        branch (bool): Selected branch. Defaults to True.
        true_branch (str): Uids of the child nodes in the
            True branch.
        false_branch (str): Uids of the child nodes in the
            False branch.
        active (bool): Internal variable to manage nested
            if/elif/else.
        to_be_dropped (bool): Internal variable to manage
            nested if/elif/else.
    """

    type: Literal["IfNode"] = "IfNode"
    branch: bool = True
    true_branch: list[str] = Field(default_factory=list)
    false_branch: list[str] = Field(default_factory=list)
    active: bool = False
    to_be_dropped: bool = False

    def add_child(self, uid: str) -> None:
        """Add a child to the correct branch.

        Args:
            uid (str): Child unique id.
        """
        if self.branch:
            self.true_branch.append(uid)
        else:
            self.false_branch.append(uid)

    def is_leaf(self) -> bool:
        """Check if the node is a leaf.

        Returns:
            bool: Always false, IfNodes cannot be leaves.
        """
        return False


class OneOfNode(BaseNodeType):
    """OneOf node.

    They have two use cases:
    1. Selecting nodes from mutually excluded branches.
    2. Selecting The first successfully completed parent.

    Attributes:
        type (Literal['OneOf']): Node type.
        children (list[str]): Children uids.
    """

    type: Literal["OneOfNode"] = "OneOfNode"
    children: list[str] = Field(default_factory=list)

    def add_child(self, uid: str) -> None:
        """Add a child to the node.

        Args:
            uid (str): Child uid.
        """
        self.children.append(uid)

    def is_leaf(self) -> bool:
        """Check if the node is a leaf.

        Returns:
            bool: True if the node is a leaf.
        """
        return not self.children


class EndNode(BaseNodeType):
    """Final node of a pipeline.

    Args:
        type (Literal['EndNode']): Node type.
        children (list[str]): Children uids.
    """

    type: Literal["EndNode"] = "EndNode"
    children: list[str] = Field(default_factory=list)

    def add_child(self, uid: str) -> None:
        """Add a child to the node.

        Args:
            uid (str): Child uid.
        """
        self.children.append(uid)

    def is_leaf(self) -> bool:
        """Check if the node is a leaf.

        Endnodes may not be leaves for pipelines
        called within other pipelines.

        Returns:
            bool: True if the node is a leaf.
        """
        return not self.children


class RootNode(BaseNodeType):
    """Pipeline root node.

    Attributes:
        type (Literal['EndNode']): Node type.
        children (list[str]): Children uids.
    """

    type: Literal["RootNode"] = "RootNode"
    children: list[str] = Field(default_factory=list)

    def add_child(self, uid: str) -> None:
        """Add a child to the node.

        Args:
            uid (str): Child uid.
        """
        self.children.append(uid)

    def is_leaf(self) -> bool:
        """Check if the node is a leaf.

        Returns:
            bool: True if the node is a leaf.
        """
        return not self.children


T = TypeVar("T", bound=BaseNodeType, covariant=True)
"""Node type."""

U = TypeVar("U", bound=BaseNodeType)
"""Node type."""


class Parent(BaseModel):
    """Node parent.

    Attributes:
        uid (str): Parent uid.
        name (str): Parent name. It is the task input key
            for parent kwargs.
    """

    uid: str
    name: str


class Node(BaseModel, Generic[T]):
    """Node.

    DAGs (aka pipelines) are made of nodes. Task nodes
    are associated with user-defined functions, but many
    other types exist.

    Attributes:
        uid (str): Node unique id.
        parents (list[Parent]): Node parents.
        behavior (BaseNodeType): Node type.
    """

    uid: str
    parents: list[Parent] = Field(default_factory=list)
    behavior: T

    def is_leaf(self) -> bool:
        """Check if the node is a leaf.

        Returns:
            bool: True if the node is a leaf.
        """
        return self.behavior.is_leaf()

    def add_edge(self, child: "Node[U]", name: str = "") -> None:
        """Add an edge to the graph.

        Both parent-child and child-parent relationships are
        tracked.

        Args:
            child (Self): Child of this node.
            name (str, optional): Child name. It is used to identify
                input kwargs. Defaults to ''.
        """
        child.add_parent(name, self)
        self._add_child(child)

    def add_parent(self, name: str, node: "Node[U]") -> None:
        """Add a parent to the current node.

        Args:
            name (str): Parent name. Used to track the input kwargs.
            node (Self): Parent node.
        """
        parent = Parent(uid=node.uid, name=name)
        self.parents.append(parent)

    def _add_child(self, node: "Node[U]") -> None:
        """Add a child to the current node.

        Args:
            node (Self): Child.
        """
        return self.behavior.add_child(node.uid)


BehaviorUnion = Annotated[
    TaskNode | IfNode | EndNode | RootNode | OneOfNode,
    Field(discriminator="type"),
]
"""Node type discriminated union."""

NodeUnion = Node[BehaviorUnion]
"""Node of all possible types."""


class Graph(BaseModel):
    """Node graph.

    This is the object exported when the pipeline is compiled.

    Attributes:
        name (str): Pipeline name. Defaults to ''.
        creation_dt (datetime): Pipeline compilation datetime.
        nodes (list[NodeUnion]): Graph nodes.
    """

    name: str = ""
    creation_dt: datetime = Field(default_factory=datetime.now)
    nodes: list[NodeUnion] = Field(default_factory=list)

    def get_root(self) -> Node:
        """Get the root node.

        Raises:
            RootNotFoundError: The root node is not found.

        Returns:
            Node: Root node.
        """
        for node in self.nodes:
            if not node.parents:
                return node

        raise RootNotFoundError

    def get_end(self) -> Node:
        """Get the terminal node.

        Raises:
            EndNotFoundError: The end node is not found.

        Returns:
            Node: End node.
        """
        for node in self.nodes:
            if node.is_leaf():
                return node

        raise EndNotFoundError
