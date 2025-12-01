"""DAGs."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

from sdag.exceptions import (
    DAGNotSetError,
    IncorrectElifError,
    IncorrectElseError,
    MissingActiveBranchError,
    TaskNotUniqueError,
)
from sdag.io import IOManager
from sdag.models import (
    EndNode,
    Graph,
    GraphMetadata,
    IfNode,
    Node,
    OneOfNode,
    RootNode,
)
from sdag.wrappers import IfWrapper, Pipeline, Task


class DAG:
    """DAG.

    Each pipeline is compiled into a DAG. Other than the graph,
    each DAG also contains a stack to keep track of nested if/else
    flows. Each pipeline has a unique entry point (root) and a
    unique end. Both are added at the end of the compilation step.

    Attributes:
        prefix (str): Prefix used for root and end nodes.
        graph (Graph): DAG graph.
        branchstack (List[Node[IfNode]]): Stack to store outer
            if blocks.
    """

    def __init__(self, uid: str, name: str = ""):
        """Initialize a new DAG.

        Args:
            uid (str): uid used to initialize root and end nodes.
            name (str, optional): DAG name. Defaults to "".
        """
        self.prefix = f"_{name}_{uid}"
        self.graph = Graph(meta=GraphMetadata(name=name))
        self.branchstack: list[Node[IfNode]] = []

    def add_root(self) -> None:
        """Add root Node to the graph."""
        uid = f"{self.prefix}_root_"
        root = Node(uid=uid, behavior=RootNode())
        for node in self.graph.nodes:
            if not node.parents:
                node.add_logical_edge(root.uid)

        self.graph.nodes.append(root)

    def add_end(self) -> None:
        """Add end node."""
        uid = f"{self.prefix}_end_"
        end = Node(uid=uid, behavior=EndNode())
        not_leaves = self.graph.find_nodes_with_children()

        for node in self.graph.nodes:
            if node.uid not in not_leaves:
                end.add_logical_edge(node.uid)

        self.graph.nodes.append(end)

    def register(self, node: Node) -> None:
        """Register a node to the DAG.

        Args:
            node (Node): Node to be registered.
        """
        self.graph.nodes.append(node)
        self._manage_if_branches(node)

    def _manage_if_branches(self, node: Node) -> None:
        if not self.branchstack:
            return

        last_branch = self.branchstack[-1]

        # We are within a branch
        if last_branch.behavior.in_context:
            node.add_branch_edge(
                last_branch.uid, branch=last_branch.behavior.branch
            )

        # Fully exited, the branch must be dropped
        elif last_branch.behavior.to_be_dropped:
            self.branchstack.pop()

        # Next time it will be dropped. We keep it for elif/else
        else:
            last_branch.behavior.to_be_dropped = True

    def register_elif_expression(self, expr: Node) -> None:
        """Register an Elif condition node.

        Args:
            expr (Node): Elif condition.

        Raises:
            MissingActiveBranchError: Active branch is missing.
            IncorrectElifError: Branch is active.
            IncorrectElifError: Not exited from the context manager.
            IncorrectElifError: Selected branch is not False.
        """
        # Elif without If
        if not self.branchstack:
            raise MissingActiveBranchError

        last_branch = self.branchstack.pop()

        # Elif without If (within a branch)
        if last_branch.behavior.in_context:
            raise IncorrectElifError

        # If/Elif not closed yet
        if not last_branch.behavior.to_be_dropped:
            raise IncorrectElifError

        last_branch.behavior.branch = False
        expr.add_branch_edge(
            last_branch.uid, branch=last_branch.behavior.branch
        )

    def push_stack(self, node: Node[IfNode]) -> None:
        """Push IfNode to the stack.

        Args:
            node (Node[IfNode]): Node to be pushed to the stack.
        """
        self.branchstack.append(node)

    def get_graph(self) -> Graph:
        """Get graph.

        Returns:
            Graph: Node graph.
        """
        self.add_root()
        self.add_end()
        return self.graph

    def join(self, graph: Graph) -> None:
        """Join a graph.

        Args:
            graph (Graph): Graph.
        """
        self.graph.nodes.extend(graph.nodes)


class SDAG:
    """Slurm DAG.

    Attributes:
        uid (int): Used to assign uids. It is not thread
            or process safe.
        taskdict (dict[str, Callable]): Map containing
            all tasks (whose name must be unique).
        self.dagstack (list[DAG]): Stack used to compile
            pipelines within other pipelines.
        self.current (DAG): DAG being compiled.
    """

    def __init__(self):
        """Initialize the object."""
        self.uid = 0
        self.taskdict: dict[str, Callable] = {}
        self.dagstack: list[DAG] = []
        self.current: DAG | None = None

    def run(self) -> None:
        """Stage entry point.

        All launch scripts must have this function as entry point.
        This object will then dispatch the call to the correct task.
        """
        manager = IOManager()
        node = manager.find_node_to_be_executed()
        fn = self.taskdict[node.behavior.fname]
        input_kwargs = manager.get_input(fn)
        artifacts = manager.get_artifacts(fn, input_kwargs)
        input_kwargs |= artifacts
        output = fn(**input_kwargs)
        manager.serialize_output(output, artifacts)

    def set_current_dag(self, name: str) -> None:
        """Mark a new DAG as the current under compilation.

        Args:
            name (str): Pipeline name.
        """
        if self.current is not None:
            self.dagstack.append(self.current)
        self.current = DAG(uid=self.get_uid(), name=name)

    def task(
        self,
        launch_script: str | Path,
        caching: bool = False,  # noqa: FBT002
        retries: int = 0,
    ) -> Callable[[Callable], Task]:
        """Task.

        A task is a deferred function that will be scheduled and
        executed.

        Args:
            launch_script (str | Path): Slurm script to execute
                the task.
            caching (bool, optional): If enabled, the task execution
                will be skipped if the task result is available.
                Defaults to False.
            retries (int): Number of retries that will be attempted.
                Default to zero.

        Returns:
            Callable[[Callable], Task]: Task wrapper.
        """

        def return_task(fn: Callable) -> Task:
            """Task wrapper.

            Args:
                fn (Callable): User-defined task function.

            Raises:
                TaskNotUniqueError: The task name is not unique.

            Returns:
                Task: Task.
            """
            if fn.__name__ in self.taskdict:
                raise TaskNotUniqueError(name=fn.__name__)

            self.taskdict[fn.__name__] = fn
            return Task(
                fn=fn,
                caching=caching,
                retries=retries,
                launch_script=Path(launch_script).resolve(),
                register=self.register,
                get_uid=self.get_uid,
            )

        return return_task

    def get_uid(self) -> str:
        """Provide a unique id.

        Many objects contain references to this
        function to safely register nodes.

        Returns:
            str: Unique id.
        """
        uid = self.uid
        self.uid += 1
        return str(uid)

    def pipeline(self, fn: Callable[[], None]) -> Pipeline:
        """Pipeline decorator.

        Args:
            fn (Callable[[], None]): User-defined pipeline function.

        Returns:
            Pipeline: Pipeline.
        """
        return Pipeline(
            fn=fn, set_dag=self.set_current_dag, get_graph=self.get_graph
        )

    def compile(
        self,
        pipeline: Pipeline,
        dst_dir: str | Path | None = None,
        name: str | None = None,
        input_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Compile the pipeline to a JSON file.

        Args:
            pipeline (Pipeline): Pipeline to be compiled.
            dst_dir (str | Path | None, optional): Destination
                directory. If none, it will be equal to the current one.
            name (str): File name. If None, it will be equal to the
                pipeline function name. Defaults to None.
            input_kwargs (dict[str, Any] | None): Pipeline static input
                arguments. Defaults to None.
        """
        self._reset_uid()
        graph = pipeline.compile(input_kwargs)
        graph_json = graph.model_dump_json(indent=4, warnings="none")

        dst_dir = Path(dst_dir) if dst_dir is not None else Path()
        fname = name if name is not None else pipeline.fn.__name__
        path = (dst_dir / fname).with_suffix(".json")

        with path.open("w") as f:
            f.write(graph_json)

    def get_graph(self) -> Graph:
        """Get the compiled pipeline graph.

        Raises:
            DAGNotSetError: DAG is not set.

        Returns:
            Graph: Compiled pipeline graph.
        """
        if self.current is None:
            raise DAGNotSetError

        graph = self.current.get_graph()
        self.current = None if not self.dagstack else self.dagstack.pop()
        if self.current is not None:
            self.current.join(graph)
        return graph

    def If(self, expr: Node) -> IfWrapper:  # noqa: N802
        """If node.

        If the task passed evaluates to True, than the branch within
            the context manager will be evaluated.

        Args:
            expr (Node): Task node. It's output must be a Boolean.

        Returns:
            IfWrapper: If task.
        """
        if self.current is None:
            raise DAGNotSetError

        node = Node(uid=self.get_uid(), behavior=IfNode())
        self.current.register(node)
        node.add_output_edge(parent_uid=expr.uid, key="expr")
        return IfWrapper(node=node, push_branch=self.push_branch)

    def Else(self) -> IfWrapper:  # noqa: N802
        """Else branch.

        Must be used after an If branch.

        Raises:
            DAGNotSetError: DAG is not set.
            MissingActiveBranchError: Last branch is not set.
            IncorrectElseError: Last branch must be dropped.
            IncorrectElseError: Last branch is still active.

        Returns:
            IfWrapper: Else task.
        """
        if self.current is None:
            raise DAGNotSetError

        if not self.current.branchstack:
            raise MissingActiveBranchError

        # The context manager repushes it
        last_branch = self.current.branchstack.pop()

        if last_branch.behavior.to_be_dropped:
            raise IncorrectElseError

        if last_branch.behavior.in_context:
            raise IncorrectElseError

        last_branch.behavior.branch = False
        last_branch.behavior.to_be_dropped = True
        return IfWrapper(node=last_branch, push_branch=self.push_branch)

    def Elif(self, expr: Node) -> IfWrapper:  # noqa: N802
        """Elif node.

        Args:
            expr (Node): Task node. It's output must be Boolean.

        Raises:
            DAGNotSetError: DAG is not set.

        Returns:
            IfWrapper: Task.
        """
        if self.current is None:
            raise DAGNotSetError

        self.current.register_elif_expression(expr)
        return self.If(expr)

    def OneOf(self, *args: Node) -> Node[OneOfNode]:  # noqa: N802
        """OneOf task.

        It has two major use cases:
        1. Selecting nodes from mutually excluded branches.
        2. Selecting The first successfully completed parent.

        Raises:
            DAGNotSetError: DAG is not set.

        Returns:
            Node[OneOfNode]: OneOf node.
        """
        if self.current is None:
            raise DAGNotSetError

        node = Node(uid=self.get_uid(), behavior=OneOfNode())
        self.current.register(node)
        for parent in args:
            node.add_logical_edge(parent.uid)
        return node

    def register(self, node: Node) -> None:
        """Register a node to the current DAG.

        Args:
            node (Node): Node to be registered.

        Raises:
            DAGNotSetError: DAG is not set.
        """
        if self.current is None:
            raise DAGNotSetError

        self.current.register(node)

    def push_branch(self, node: Node[IfNode]) -> None:
        """Push a branch to the stack.

        Args:
            node (Node[IfNode]): If node to be pushed.

        Raises:
            DAGNotSetError: DAG is not set.
        """
        if self.current is None:
            raise DAGNotSetError

        self.current.push_stack(node)

    def _reset_uid(self) -> None:
        """Rest sdag starting uid to get deterministic uids."""
        self.uid = 0
