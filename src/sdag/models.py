"""DAG model."""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Generic, Literal, TypeVar

from pydantic import BaseModel, Field, PrivateAttr

from sdag.exceptions import EndNotFoundError, RootNotFoundError


class Artifact(BaseModel):
    """Artifact.

    Attributes:
        name (str): Artifact name.
        path (Path | str): Artifact path.
    """

    name: str
    path: Path | str = Field(default_factory=Path)


class TaskOutput(BaseModel):
    """Output of a task.

    Attributes:
        output (Any): Task output.
        artifacts (list[Artifact]): Output artifacts.
    """

    output: Any
    artifacts: list[Artifact]


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
        input_kwargs (list[InputKwarg]): Static input kwargs.
        children (list[str]): Children uids.
    """

    type: Literal["TaskNode"] = "TaskNode"
    fname: str
    launch_script: Path
    caching: bool
    retries: int
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
            false branch.
        in_context (bool): True if the context manager is
            active (so we are within the branch). Defaults
            to True.
        to_be_dropped (bool): Internal variable to manage
            nested if/elif/else. Defaults to False.
    """

    type: Literal["IfNode"] = "IfNode"
    branch: bool = True
    true_branch: list[str] = Field(default_factory=list)
    false_branch: list[str] = Field(default_factory=list)
    in_context: bool = False
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


class ParentType(BaseModel):
    """Base class for all parent types."""


class LogicalType(ParentType):
    """Node logical parent.

    The dependence is logical, there is no I/O exchange.

    Attributes:
        type (Literal['Logical']): Parent type.
    """

    type: Literal["Logical"] = "Logical"


class ArtifactType(ParentType):
    """Artifact relashionship.

    The node depends on the parent via an artifact.

    Attributes:
        type (Literal['Artifact']): Parent type.
        key (str): Artifact key in the node input kwargs.
        name (str): Artifact name.
    """

    type: Literal["Artifact"] = "Artifact"
    key: str
    name: str


class OutputType(ParentType):
    """Output relationship.

    Node depends on the parent output.

    Attributes:
        type (Literal['Output']): Parent type.
        key (str): Key in the node input kwargs.
    """

    type: Literal["Output"] = "Output"
    key: str


V = TypeVar("V", bound=ParentType, covariant=True)
"""Node type."""


class Parent(BaseModel, Generic[V]):
    """Node parent.

    Attributes:
        parent_type (V): Relationship type.
        uid (str): Parent uid.
    """

    parent_type: V
    uid: str


class ArtifactContainer:
    """Artifact wrapper.

    Hack to pass the node to the child alongside
    artifacts.

    Attributes:
        key (str): Artifact key.
        node (Node): Parent node.
    """

    def __init__(self, key: str, node: "Node"):
        """Initialize the artifact container.

        Args:
            key (str): Artifact key.
            node (Node): Parent node.
        """
        self.key = key
        self.node = node


ParentTypeUnion = Annotated[
    LogicalType | ArtifactType | OutputType,
    Field(discriminator="type"),
]
"""Node type discriminated union."""

ParentUnion = Parent[ParentTypeUnion]
"""Node of all possible types."""


class Node(BaseModel, Generic[T]):
    """Node.

    DAGs (aka pipelines) are made of nodes. Task nodes
    are associated with user-defined functions, but many
    other types exist.

    Attributes:
        uid (str): Node unique id.
        parents (list[Parent]): Node parents.
        behavior (BaseNodeType): Node type.
        _artifacts (dict[str, ArtifactContainer]): Artifact
            keys and wrappers.
    """

    uid: str
    parents: list[ParentUnion] = Field(default_factory=list)
    behavior: T
    _artifacts: dict[str, ArtifactContainer] = PrivateAttr(
        default_factory=dict
    )

    @property
    def artifacts(self) -> dict[str, ArtifactContainer]:
        """Get the artifacts.

        Returns:
            dict[str, ArtifactContainer]: Artifacts.
        """
        return self._artifacts

    def register_artifact(self, key: str) -> None:
        """Register an artifact.

        Args:
            key (str): Artifact key.
        """
        self._artifacts[key] = ArtifactContainer(key, self)

    def is_leaf(self) -> bool:
        """Check if the node is a leaf.

        Returns:
            bool: True if the node is a leaf.
        """
        return self.behavior.is_leaf()

    def add_logical_edge(self, child: "Node[U]") -> None:
        """Add a logical edge.

        It is a logical dependence, no data exchange.

        Args:
            child (Node[U]): Child node.
        """
        parent = Parent(uid=self.uid, parent_type=LogicalType())
        child.parents.append(parent)
        self._add_child(child)

    def add_output_edge(self, child: "Node[U]", key: str) -> None:
        """Add output edge.

        Child will read the parent output.

        Args:
            child (Node[U]): Child node.
            key (str): parent output key in the child input kwargs.
        """
        parent = Parent(uid=self.uid, parent_type=OutputType(key=key))
        child.parents.append(parent)
        self._add_child(child)

    def add_artifact_edge(self, child: "Node[U]", key: str, name: str) -> None:
        """Add an artifact edge.

        The child will use an artifact produced by the parent.

        Args:
            child (Node[U]): Child node.
            key (str): Artifact key in the child input kwargs.
            name (str): Artifact name.
        """
        parent = Parent(
            uid=self.uid, parent_type=ArtifactType(key=key, name=name)
        )
        child.parents.append(parent)
        self._add_child(child)

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
