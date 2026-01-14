"""DAG model."""

import logging
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, Generic, Literal, TypeVar

from pydantic import BaseModel, Field, PrivateAttr
from pydantic_settings import BaseSettings

from sdag.exceptions import EndNotFoundError, RootNotFoundError

logger = logging.getLogger(__name__)


class CompileSettings(BaseSettings):
    """Compilation environment variables.

    Attributes:
        sdag_base_path (Path | None): Base path. Defaults to None.
    """

    sdag_base_path: Path | None = None


@lru_cache
def get_compile_settings() -> CompileSettings:
    """Get cached compilation settings.

    Returns:
        CompileSettings: Cached compilation settings.
    """
    return CompileSettings()


class Artifact(BaseModel):
    """Artifact.

    Attributes:
        name (str): Artifact name.
        path (Path): Artifact path.
    """

    name: str
    path: Path


class TaskOutput(BaseModel):
    """Output of a task.

    Attributes:
        output (Any): Task output.
        artifacts (list[Artifact]): Output artifacts.
    """

    output: Any
    artifacts: list[Artifact]


class BaseNodeType(BaseModel):
    """Base class for all nodes."""


class InputKwarg(BaseModel):
    """Task node static input kwargs.

    They are copied as JSON strings in the pipeline JSON.

    Attributes:
        key (str): Input name.
        value (Any): Input value.
    """

    key: str
    value: Any


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
    """

    type: Literal["TaskNode"] = "TaskNode"
    fname: str
    launch_script: Path
    caching: bool
    retries: int
    input_kwargs: list[InputKwarg] = Field(default_factory=list)


class IfNode(BaseNodeType):
    """Node associated to a conditional.

    Attributes:
        type (Literal['IfNode']): Node type.
        branch (bool): Selected branch. Defaults to True.
        in_context (bool): True if the context manager is
            active (so we are within the branch). Defaults
            to True.
        to_be_dropped (bool): Internal variable to manage
            nested if/elif/else. Defaults to False.
    """

    type: Literal["IfNode"] = "IfNode"
    branch: bool = True
    in_context: bool = False
    to_be_dropped: bool = False


class OneOfNode(BaseNodeType):
    """OneOf node.

    They have two use cases:
    1. Selecting nodes from mutually excluded branches.
    2. Selecting The first successfully completed parent.

    Attributes:
        type (Literal['OneOf']): Node type.
    """

    type: Literal["OneOfNode"] = "OneOfNode"


class EndNode(BaseNodeType):
    """Final node of a pipeline.

    Args:
        type (Literal['EndNode']): Node type.
    """

    type: Literal["EndNode"] = "EndNode"


class RootNode(BaseNodeType):
    """Pipeline root node.

    Attributes:
        type (Literal['EndNode']): Node type.
        children (list[str]): Children uids.
    """

    type: Literal["RootNode"] = "RootNode"


T = TypeVar("T", bound=BaseNodeType, covariant=True)
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
        path (Path): Artifact path.
    """

    type: Literal["Artifact"] = "Artifact"
    key: str
    name: str
    path: Path


class OutputType(ParentType):
    """Output relationship.

    Node depends on the parent output.

    Attributes:
        type (Literal['Output']): Parent type.
        key (str): Key in the node input kwargs.
    """

    type: Literal["Output"] = "Output"
    key: str


class BranchType(ParentType):
    """Branch relationship.

    Node depends on one of the two branches of the parent.

    Attributes:
        type (Literal['If']): Parent type.
        branch (bool): Branch the child depends on.
    """

    type: Literal["Branch"] = "Branch"
    branch: bool


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
        path (str): Artifact path.
        node (Node): Parent node.
    """

    def __init__(self, key: str, path: Path, node: "Node"):
        """Initialize the artifact container.

        Args:
            key (str): Artifact key.
            path (str): Artifact path.
            node (Node): Parent node.
        """
        self.key = key
        self.path = path
        self.node = node


ParentTypeUnion = Annotated[
    LogicalType | ArtifactType | OutputType | BranchType,
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
    output_artifacts: list[Artifact] = Field(default_factory=list)
    _artifact_containers: dict[str, ArtifactContainer] = PrivateAttr(
        default_factory=dict
    )

    @property
    def artifacts(self) -> dict[str, ArtifactContainer]:
        """Get the artifacts.

        Returns:
            dict[str, ArtifactContainer]: Artifacts.
        """
        return self._artifact_containers

    def register_artifact(self, key: str, path: Path) -> None:
        """Register an artifact.

        If the `SDAG_BASE_PATH` environment variable is set and
        path is not absolute, the registered path is appended to
        the base path.

        Args:
            key (str): Artifact key.
            path (Path): Artifact path.
        """
        settings = get_compile_settings()
        if not path.is_absolute() and settings.sdag_base_path is not None:
            path = settings.sdag_base_path / path

        self.output_artifacts.append(Artifact(name=key, path=path))
        self._artifact_containers[key] = ArtifactContainer(key, path, self)

    def add_logical_edge(self, parent_uid: str) -> None:
        """Add a child->parent logical edge.

        It is a logical dependence, no data exchanged.

        Args:
            parent_uid (str): Parent uid.
        """
        parent = Parent(uid=parent_uid, parent_type=LogicalType())
        self.parents.append(parent)

    def add_output_edge(self, parent_uid, key: str) -> None:
        """Add a child->parent output edge.

        Child will read the parent output.

        Args:
            parent_uid (str): Parent uid.
            key (str): parent output key in the child input kwargs.
        """
        parent = Parent(uid=parent_uid, parent_type=OutputType(key=key))
        self.parents.append(parent)

    def add_artifact_edge(
        self, parent_uid: str, key: str, name: str, path: Path
    ) -> None:
        """Add a child->parent artifact edge.

        The child will use an artifact produced by the parent.

        Args:
            parent_uid (str): Parent uid.
            key (str): Artifact key in the child input kwargs.
            name (str): Artifact name.
            path (Path): Artifact path.
        """
        parent = Parent(
            uid=parent_uid,
            parent_type=ArtifactType(key=key, name=name, path=path),
        )
        self.parents.append(parent)

    def add_branch_edge(self, parent_uid: str, branch: bool) -> None:
        """Add a child->parent branch edge.

        The child depends on one of the branches of an IfNode.

        Args:
            parent_uid (str): Parent uid.
            branch (bool): Branch the child depeds on.
        """
        parent = Parent(uid=parent_uid, parent_type=BranchType(branch=branch))
        self.parents.append(parent)

    def join_artifact_containers(
        self, artifacts: dict[str, ArtifactContainer]
    ) -> None:
        """Join artifact containers without setting an edge.

        Used to make all artifacts available to the pipeline end node.

        Args:
            artifacts (dict[str, ArtifactContainer]):
                Artifact containers.
        """
        for name, artifact in artifacts.items():
            if name in self._artifact_containers:
                logger.info(
                    "Detected duplicate artifact name '%s'. This is fine"
                    " in most cases but it will be overwritten if accessed"
                    " through the pipeline task.",
                    name,
                )

            self._artifact_containers[name] = artifact


BehaviorUnion = Annotated[
    TaskNode | IfNode | EndNode | RootNode | OneOfNode,
    Field(discriminator="type"),
]
"""Node type discriminated union."""

NodeUnion = Node[BehaviorUnion]
"""Node of all possible types."""


class GraphMetadata(BaseModel):
    """Graph metadata.

    Attributes:
        name (str): Pipeline name.
        creation_dt (datetime): Pipeline compilation datetime.
    """

    name: str
    creation_dt: datetime = Field(default_factory=datetime.now)


class Graph(BaseModel):
    """Node graph.

    This is the object exported when the pipeline is compiled.

    Attributes:
        meta (PipelineMetadata): Pipeline metadata.
        nodes (list[NodeUnion]): Graph nodes.
    """

    meta: GraphMetadata
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
        not_leaves = self.find_nodes_with_children()
        for node in self.nodes:
            if node.uid not in not_leaves:
                return node

        raise EndNotFoundError

    def find_nodes_with_children(self) -> set[str]:
        """Find the uids of all nodes with children.

        Useful to identify leaves.

        Returns:
            set[str]: Nodes without children.
        """
        return {p.uid for node in self.nodes for p in node.parents}
