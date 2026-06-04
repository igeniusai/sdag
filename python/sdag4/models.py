"""sdag models."""

import random
import sys
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal, TypeVar

from pydantic import (
    BaseModel,
    Field,
    PrivateAttr,
    field_validator,
    model_validator,
)

from sdag4.constants import POSIX_ENV_VARIABLES
from sdag4.exceptions import POSIXOverrideError

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class ScriptPath(BaseModel):
    """Script path.

    Attributes:
        kind (Literal['script_path']): Script path.
        path (Path): Path.
    """

    kind: Literal["script_path"] = "script_path"
    path: Path

    @field_validator("path")
    @classmethod
    def expand_if_relative(cls, v: Path):
        """Expand the script path.

        Args:
            v (Path): Script path.

        Returns:
            Path: Expanded path.
        """
        if not v.absolute():
            v = v.expanduser()
        return v


class ScriptContent(BaseModel):
    """Script embedded in the pipeline definition.

    Attributes:
        kind (Literal['script']): Script kind.
        content (str): Script content.
    """

    kind: Literal["script"] = "script"
    content: str


ScriptUnion = Annotated[
    ScriptContent | ScriptPath, Field(discriminator="kind")
]
"""Script content or path."""


class ArtifactSig:
    """Used to mark artifacts."""


T = TypeVar("T", bound=str | Path)
Artifact = Annotated[T, ArtifactSig]
"""Artifact.

Artifacts are used to signal that some assets are being
written. Cache is invalidated if artifacts are missing.
It is possible to use either Artifact[str] or Artifact[Path],
they are casted automatically.
"""


class Kwarg(BaseModel):
    """Task input kwarg.

    Attributes:
        key (str): Key.
        value (Any): Value.
    """

    key: str
    value: Any


class ArtifactEdge(BaseModel):
    """Artifact edge.

    Attributes:
        name (str): Artifact name.
        path (Path): Artifact path.
    """

    name: str
    path: Path


class LogicalParent(BaseModel):
    """Logical parent kind.

    Attributes:
        kind (Literal["logical"]): Kind.
    """

    kind: Literal["logical"] = "logical"


class OutputParent(BaseModel):
    """Output edge.

    Attributes:
        kind (Literal["outout"]): Kind.
        key (str): Field name.
    """

    kind: Literal["output"] = "output"
    key: str


class ArtifactParent(BaseModel):
    """Artifact edge.

    Attributes:
        kind (Literal["artifact"]): Artifact kind.
        key (str): Artifact key.
        name (str): Artifact name.
        path (str): Artifact path.
    """

    kind: Literal["artifact"] = "artifact"
    key: str
    name: str
    path: Path


class BranchParent(BaseModel):
    """Branch edge.

    Attributes:
        kind (Literal["branch"]): Kind.
        branch (bool): Branch.
    """

    kind: Literal["branch"] = "branch"
    branch: bool


class TaskOutput(BaseModel):
    """Output of a task.

    Attributes:
        output (Any): Task output.
        artifacts (list[ArtifactEdge]): Output artifacts.
    """

    output: Any
    artifacts: list[ArtifactEdge]


ParentKindUnion = Annotated[
    LogicalParent | OutputParent | ArtifactParent | BranchParent,
    Field(discriminator="kind"),
]
"""Parent types."""


class Parent(BaseModel):
    """Edge in the graph.

    Attributes:
        uid (int): Parent type.
        kind (str): Edge type.
    """

    uid: int
    kind: ParentKindUnion


class ArtifactContainer:
    """Artifact wrapper.

    Hack to pass the node to the child alongside
    artifacts.

    Attributes:
        key (str): Artifact key.
        path (str): Artifact path.
        node (Node): Parent node.
    """

    def __init__(self, key: str, path: Path, node: "TaskNode"):
        """Initialize the artifact container.

        Args:
            key (str): Artifact key.
            path (str): Artifact path.
            node (Node): Parent node.
        """
        self.key = key
        self.path = path
        self.node = node


class BaseNode(BaseModel):
    """Node base class.

    Attributes:
        parents (list[Parent]): Parents.
        uid (int): Unique id.
        root_uid (int) Pipeline root unique id (added
        during compilation. Defaults to -1.
        pipeline_name (str) = Pipeline name, added during
            compilation. Defaults to ''.
    """

    parents: list[Parent] = Field(default_factory=list)
    uid: int
    root_uid: int = -1
    pipeline_name: str = ""

    def add_logical_edge(self, parent_uid: int) -> None:
        """Add a child->parent logical edge.

        It is a logical dependence, no data exchanged.

        Args:
            parent_uid (int): Parent uid.
        """
        parent = Parent(uid=parent_uid, kind=LogicalParent())
        self.parents.append(parent)

    def add_branch_edge(self, parent_uid: int, branch: bool) -> None:
        """Add a child->parent branch edge.

        The child depends on one of the branches of an IfNode.

        Args:
            parent_uid (int): Parent uid.
            branch (bool): Branch the child depends on.
        """
        parent = Parent(uid=parent_uid, kind=BranchParent(branch=branch))
        self.parents.append(parent)


class SlurmOverride(BaseModel):
    """Override Slurm arguments.

    They must be set following the Slurm conventions.

    Attributes:
        job_name: (str | None): Job name. Defaults to None.
        nodes: (int | None): Number of nodes. Defaults to None.
        partition: (str | None): Partition. Defaults to None.
        qos: (str | None) = qos. Defaults to None.
        gpus_per_node: (int | None): GPUs per node. Defaults to None.
        ntasks_per_node: (int | None): Tasks per node.
            Defaults to None.
        output: (str | None): stdout. Defaults to None.
        error: (str | None): stderr. Defaults to None.
        account: (str | None): Account. Defaults to None.
        cpus_per_task (str | None): CPUs per task. Defaults to None.
        mem: (str | None): Memory. Defaults to None.
        time: (str | None): Wall time. Defaults to None.
    """

    job_name: str | None = None
    nodes: int | None = None
    partition: str | None = None
    qos: str | None = None
    gpus_per_node: int | None = None
    ntasks_per_node: int | None = None
    output: str | None = None
    error: str | None = None
    account: str | None = None
    cpus_per_task: str | None = None
    mem: str | None = None
    time: str | None = None


class TaskNode(BaseNode):
    """Task.

    Attributes:
        kind (Literal["task"]): Kind.
        parents (list[Parent]): Edges.
        fn_name (str): Function name.
        name (str): Task name, linked to caching.
        cache (bool): Cache the task locally.
        cache_local (bool): Cache task gloabally.
        debug (bool): Block the scheduler in case of failures.
        mode (Literal["wrap", "ext"]): Wrap a Python function or
            an external script.
        cmd (Literal["bash", "sbatch"]): Command used to launch
            the script.
        retries (int): Retries.
        script (ScriptUnion): Script path or content.
        kwargs (list[Kwarg]): Input kwargs.
        override (SlurmOverride): Override Slurm resources.
        output_artifacts (list[ArtifactEdge]): Artifacts.
        _artifact_containers (dict[str, ArtifactContainer]):
            Used to add edges to the graph.
    """

    kind: Literal["task"] = "task"
    parents: list[Parent] = Field(default_factory=list)
    fn_name: str
    name: str
    cache: bool
    cache_local: bool
    debug: bool
    mode: Literal["wrap", "ext"]
    cmd: Literal["bash", "sbatch"]
    retries: int
    script: ScriptUnion
    kwargs: list[Kwarg] = Field(default_factory=list)
    override: SlurmOverride = Field(
        default_factory=SlurmOverride, serialization_alias="slurm_override"
    )
    output_artifacts: list[ArtifactEdge] = Field(
        default_factory=list, serialization_alias="artifacts"
    )
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

        Args:
            key (str): Artifact key.
            path (Path): Artifact path.
        """
        self.output_artifacts.append(ArtifactEdge(name=key, path=path))
        self._artifact_containers[key] = ArtifactContainer(key, path, self)

    def add_output_edge(self, parent_uid, key: str) -> None:
        """Add a child->parent output edge.

        Child will read the parent output.

        Args:
            parent_uid (int): Parent uid.
            key (str): parent output key in the child input kwargs.
        """
        parent = Parent(uid=parent_uid, kind=OutputParent(key=key))
        self.parents.append(parent)

    def add_artifact_edge(
        self, parent_uid: int, key: str, name: str, path: Path
    ) -> None:
        """Add a child->parent artifact edge.

        The child will use an artifact produced by the parent.

        Args:
            parent_uid (int): Parent uid.
            key (str): Artifact key in the child input kwargs.
            name (str): Artifact name.
            path (Path): Artifact path.
        """
        parent = Parent(
            uid=parent_uid,
            kind=ArtifactParent(key=key, name=name, path=path),
        )
        self.parents.append(parent)

    def add_kwarg(self, kwarg: Kwarg) -> None:
        """Add a static kwarg.

        Args:
            kwarg (Kwarg): Kwarg to be added.

        Raises:
            POSIXOverrideError: Task is external and variable is a
                common POSIX variable.
        """
        if self.mode == "ext" and kwarg.key.upper() in POSIX_ENV_VARIABLES:
            raise POSIXOverrideError(task_name=self.name, key=kwarg.key)
        self.kwargs.append(kwarg)


class RootNode(BaseNode):
    """Root node.

    Attributes:
        kind (Literal["root"]): Kind.
        root_uid: Same as uid.
    """

    kind: Literal["root"] = "root"
    root_uid: int = Field(alias="uid", default=-1)


class BranchNode(BaseNode):
    """Branch node.

    Attributes:
        kind (Literal["branch"]): Kind.
        _children (set[int]): Used to track which nodes
            have an explicit edge.
        branch (bool): Branch.
        in_context (bool): Used to mark the branch as active.
        to_be_dropped (bool): If True, the branch will be
            dropped at the successive addition.
    """

    kind: Literal["branch"] = "branch"
    _children: set[int] = PrivateAttr(default_factory=set)
    branch: bool = True
    in_context: bool = False
    to_be_dropped: bool = False

    @property
    def children(self) -> set[int]:
        """Direct children.

        Returns:
            set[int]: Children.
        """
        return self._children


class OneOfNode(BaseNode):
    """OneOf node.

    Attributes:
        kind (Literal["oneof"]): Kind.
        name (str): Node name.
        output_artifacts (list[ArtifactEdge]): Artifact edge.
        _artifact_containers (dict[str, ArtifactContainer]):
            Used to make artifact edges.
    """

    kind: Literal["oneof"] = "oneof"
    name: str
    output_artifacts: list[ArtifactEdge] = Field(
        default_factory=list, serialization_alias="artifacts"
    )
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

    def add_logical_edge(self, parent_uid: int) -> None:
        """Add a child->parent logical edge.

        It is a logical dependence, no data exchanged.

        Args:
            parent_uid (int): Parent uid.
        """
        parent = Parent(uid=parent_uid, kind=LogicalParent())
        self.parents.append(parent)


class EndNode(BaseNode):
    """End node.

    Attributes:
        kind (Literal["end"]): Kind.
        _artifact_container (dict[str, dict[str, ArtifactContainer]]):
            Used to make artifact edges.
    """

    kind: Literal["end"] = "end"
    _artifact_container: dict[str, dict[str, ArtifactContainer]] = PrivateAttr(
        default_factory=dict
    )

    @property
    def artifacts(self) -> dict[str, dict[str, ArtifactContainer]]:
        """Artifacts.

        Returns:
            dict[str, dict[str, ArtifactContainer]]: Artifacts.
        """
        return self._artifact_container

    def join_artifacts(self, node: TaskNode | OneOfNode) -> None:
        """Join all artifacts of the pipeline.

        Args:
            node (TaskNode | OneOfNode): Node.
        """
        self._artifact_container[node.name] = node.artifacts


NodeUnion = Annotated[
    TaskNode | RootNode | EndNode | BranchNode | OneOfNode,
    Field(discriminator="kind"),
]
"""Node union."""


def get_random_hash() -> str:
    """Generate an unique hash for the compiled JSON.

    Returns:
        str: Hash.
    """
    random.seed()
    return format(random.getrandbits(64), "x")


class DAGMeta(BaseModel):
    """DAG metadata.

    Attributes:
        pipeline_name (str): Pipeline name.
        hash (str): Unique hash.
        timestamp (str): Timestamp.
        import_path (str): Import path.
        extra (str): Extra metadata.
    """

    pipeline_name: str
    hash: str = Field(default_factory=get_random_hash)
    timestamp: str = Field(default_factory=datetime.now().isoformat)
    import_path: str = ""
    extra: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def set_path_to_pipeline_name_if_missing(self) -> Self:
        """Set the import path to the pipeline name if null.

        Returns:
            Self: Metadata.
        """
        if not self.import_path:
            self.import_path = self.pipeline_name
        return self


class DAG(BaseModel):
    """Compiled pipeline.

    Attributes:
        meta (DAGMeta): DAG metadata.
        nodes (list[NodeUnion]): Nodes.
    """

    meta: DAGMeta
    nodes: list[NodeUnion] = Field(default_factory=list)


class CompiledDAG(BaseModel):
    """Pipeline being compiled.

    Attributes:
        dag (DAG): DAG.
        root (RootNode): Root node.
        end (EndNode): End node.
        branches: list[BranchNode]: Branches.
    """

    dag: DAG
    root: RootNode
    end: EndNode
    branches: list[BranchNode] = Field(default_factory=list)
