"""Compiler.

It relies on two global objects everyone has
access to, so it is not thread safe.
"""

import logging
from typing import TYPE_CHECKING

from sdag4.exceptions import DAGNotSetError, TaskNotUniqueError
from sdag4.models import (
    DAG,
    BranchNode,
    CompiledDAG,
    DAGMeta,
    EndNode,
    NodeUnion,
    Parent,
    RootNode,
)

if TYPE_CHECKING:
    from sdag4.wrappers import Pipeline, Task

logger = logging.getLogger(__name__)


class SDAG:
    """Master.

    Keeps track of the imported pipelines and tasks.
    An error is raised is two tasks with the same name
    are imported.

    Attributes:
        tasks (dict[str, Task]): Imported (global) tasks.
        pipelines (dict[str, Pipeline]): Imported pipelines.
    """

    def __init__(self):
        """Initialize the master."""
        self.tasks: dict[str, Task] = {}
        self.pipelines: dict[str, Pipeline] = {}

    def add_pipeline(self, pipeline: "Pipeline") -> None:
        """Add a pipeline.

        Args:
            pipeline (Pipeline): Pipeline imported.
        """
        self.pipelines[pipeline.fn.__name__] = pipeline

    def add_task(self, task: "Task") -> None:
        """Add a task.

        Args:
            task (Task): Add a task.

        Raises:
            TaskNotUniqueError: The task is not unique.
        """
        if task.fn.__name__ in self.tasks:
            raise TaskNotUniqueError(name=task.fn.__name__)
        self.tasks[task.fn.__name__] = task

    def _reset(self) -> None:
        self.tasks = {}
        self.pipelines = {}


class DAGCompiler:
    """Compiler.

    It does most of the magic, keeps track of
    the graph and updates it when tasks are
    executed.

    Attributes:
        outer (list[CompiledDAG]): Pipelines being compiled.
        active (CompiledDAG | None): Active pipeline compiled.
        _uid (int): Used to assign monotonically increasing uids.
    """

    def __init__(self):
        """Initialize the compiler."""
        self.outer: list[CompiledDAG] = []
        self.active: CompiledDAG | None = None
        self._uid = 0

    def reset(self) -> None:
        """Reset the compiler state.

        Called before every compiled pipeline.
        """
        self.outer = []
        self.active = None
        self._uid = 0

    def get_uid(self) -> int:
        """Get a new uid.

        Returns:
            int: Unique id.
        """
        uid = self._uid
        self._uid += 1
        return uid

    def register(self, node: NodeUnion) -> None:
        """Register a node in the graph.

        Args:
            node (NodeUnion): Node.

        Raises:
            DAGNotSetError: Attempting to register a node
                but no pipelines are being compiled.
        """
        if self.active is None:
            raise DAGNotSetError

        if not node.pipeline_name:
            name = self.active.dag.meta.pipeline_name
            node.pipeline_name = name
            node.root_uid = self.active.root.uid

        self.active.dag.nodes.append(node)
        self._handle_branches(node)

    def register_branch(self, branch: BranchNode) -> None:
        """Register a branch.

        Args:
            branch (BranchNode): Branch.

        Raises:
            DAGNotSetError: Attempting to register a node
                but no pipelines are being compiled.
        """
        if self.active is None:
            raise DAGNotSetError

        self.register(branch)
        self.active.branches.append(branch)

    def set_dag(self, root: RootNode, import_path: str | None = None) -> None:
        """Set a new dag.

        Every time a new pipeline is called.

        Args:
            root (RootNode): Pipeline root node.
            import_path (str | None, optional): Pipeline import path.
                If null, it defaults to the pipeline name.
                Defaults to None.
        """
        name = root.pipeline_name
        import_path = import_path if import_path is not None else name
        dag = DAG(meta=DAGMeta(pipeline_name=name, import_path=import_path))
        end = EndNode(
            uid=self.get_uid(), root_uid=root.uid, pipeline_name=name
        )
        active = CompiledDAG(dag=dag, root=root, end=end)

        if self.active is not None:
            self.outer.append(self.active)

        self.active = active
        self.register(root)

    def get_dag(self) -> tuple[DAG, RootNode, EndNode]:
        """Detach a dag from the compiler.

        Raises:
            DAGNotSetError: No DAGs are set.

        Returns:
            tuple[DAG, RootNode, EndNode]: Graph, root, and endnode.
        """
        if self.active is None:
            raise DAGNotSetError

        dag = self.active.dag
        root = self.active.root
        end = self.active.end
        parent_uids = set()

        for node in dag.nodes:
            if not node.parents and node.uid not in (root.uid, end.uid):
                node.add_logical_edge(root.uid)
            for parent in node.parents:
                parent_uids.add(parent.uid)

        for node in dag.nodes:
            if node.uid not in parent_uids:
                end.add_logical_edge(node.uid)
        self.register(end)

        if self.outer:
            self.active = self.outer.pop()
            self.active.dag.nodes.extend(dag.nodes)

            if self.active.branches:
                branch = self.active.branches[-1]
                # TODO: This is hacky and slow
                if (
                    branch.in_context
                    and branch.root_uid == self.active.root.uid
                    and self._parents_do_not_depend_on_branch(root.parents)
                ):
                    self.add_branch_edge(root)

        else:
            self.active = None

        return dag, root, end

    def _parents_do_not_depend_on_branch(self, parents: list[Parent]) -> None:
        """Traverse the graph to check root dependencies.

        Ugly solution to prevent useless edges if two subpipelines
        live in the context of a branch (Check out the compiler tests).

        Args:
            parents (list[Parent]): Root parents.
        """
        branch_uid = self.active.branches[-1].uid
        queue: list[int] = [parent.uid for parent in parents]
        visited = set()
        nodes = {node.uid: node for node in self.active.dag.nodes}

        while queue:
            uid = queue.pop()
            node = nodes[uid]
            if node.root_uid not in visited:
                visited.add(node.root_uid)
                root = nodes[node.root_uid]
                for parent in root.parents:
                    if parent.uid == branch_uid:
                        return False
                    queue.append(parent.uid)
        return True

    def _handle_branches(self, node: NodeUnion) -> None:
        """Check if branches must be dropped etc.

        Args:
            node (NodeUnion): Node being registered.

        Raises:
            DAGNotSetError: No DAGs being compiled.
        """
        if self.active is None:
            raise DAGNotSetError

        if not self.active.branches:
            return

        branch = self.active.branches[-1]
        if branch.in_context and not self.is_child_of_branch(node):
            self.add_branch_edge(node)

        elif branch.to_be_dropped:
            self.active.branches.pop()

        else:
            branch.to_be_dropped = True

    def is_child_of_branch(self, node: NodeUnion) -> bool:
        """Check if a child already depends on the branch.

        Args:
            node (NodeUnion): Node being registered.

        Raises:
            DAGNotSetError: No DAGs currently being compiled.

        Returns:
            bool: True if the child already depends on the branch
                so any additional edge would be useless.
        """
        if self.active is None:
            raise DAGNotSetError

        branch = self.active.branches[-1]
        parents = {p.uid for p in node.parents}
        is_direct_child = branch.uid in parents
        intersect = parents.intersection(branch.children)

        return is_direct_child or bool(intersect)

    def add_branch_edge(self, node: NodeUnion) -> None:
        """Add a branch edge.

        Args:
            node (NodeUnion): Node.

        Raises:
            DAGNotSetError: No DAGs are set.
        """
        if self.active is None:
            raise DAGNotSetError

        branch = self.active.branches[-1]
        branch.children.add(node.uid)
        node.add_branch_edge(branch.uid, branch=branch.branch)


compiler = DAGCompiler()
"""Compiler."""


master = SDAG()
"""Keeps track of imported pipelines and tasks."""
