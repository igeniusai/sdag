"""DAGs."""

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from sdag.io import IOManager
from sdag.models import EndNode, Graph, IfNode, Node, OneOfNode, RootNode
from sdag.tasks import IfTask, Pipeline, Task


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
        last_branch (Node[IfNode] | None): Temporary reference to
            the last branch used. It is required internally to
            handle Elifs.
    """

    def __init__(self, uid: str, name: str = ""):
        """Initialize a new DAG.

        Args:
            uid (str): uid used to initialize root and end nodes.
            name (str, optional): DAG name. Defaults to "".
        """
        self.prefix = f"_{name}_{uid}"
        self.graph = Graph()
        self.branchstack: list[Node[IfNode]] = []
        self.last_branch: Node[IfNode] | None = None

    def add_root(self) -> None:
        """Add root Node to the graph."""
        uid = f"{self.prefix}_root_"
        root = Node(uid=uid, behavior=RootNode(type="RootNode"))
        for node in self.graph.nodes:
            if not node.parents:
                root.add_edge(node)
        self.graph.nodes.append(root)

    def add_end(self) -> None:
        """Add end node."""
        uid = f"{self.prefix}_end_"
        end = Node(uid=uid, behavior=EndNode(type="EndNode"))
        for node in self.graph.nodes:
            if node.is_leaf():
                node.add_edge(end)
        self.graph.nodes.append(end)

    def register(self, node: Node) -> None:
        """Register a node to the DAG.

        Args:
            node (Node): Node to be registered.
        """
        if self.last_branch is not None:
            if self.last_branch.behavior.active:
                self.last_branch.add_edge(node)
            elif self.last_branch.behavior.to_be_dropped:
                self.last_branch = None
            else:
                self.last_branch.behavior.to_be_dropped = True

        self.graph.nodes.append(node)

    def register_elif_expression(self, expr: Node) -> None:
        """Register an Elif expression.

        Args:
            expr (Node): Elif condition.
        """
        if self.last_branch is None or self.last_branch.behavior.active:
            msg = "Calling Elif before If"
            raise ValueError(msg)

        if not self.last_branch.behavior.to_be_dropped:
            msg = "Conditional statement must be a task"
            raise ValueError(msg)

        if not self.last_branch.behavior.branch:
            msg = "Calling Elif after Else"
            raise ValueError(msg)

        self.last_branch.behavior.branch = False
        self.last_branch.add_edge(expr)

    def pop_stack(self) -> None:
        """Remove an IfNode from the stack."""
        self.last_branch = self.branchstack.pop()

    def push_stack(self, node: Node[IfNode]) -> None:
        """Push IfNode to the stack.

        Args:
            node (Node[IfNode]): Node to be pushed to the stack.
        """
        self.last_branch = node
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

        All launch scripts must have this function as
        entry point. This object will then dispatch
        the call to the correct task.
        """
        manager = IOManager()
        node = manager.find_node_to_be_executed()
        fn = self.taskdict[node.behavior.fname]
        input_kwargs = manager.get_input(node, fn)
        output = fn(**input_kwargs)
        manager.serialize_output(output, node.uid)

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
                ValueError: The task name is not unique.

            Returns:
                Task: Task.
            """
            if fn.__name__ in self.taskdict:
                msg = "Task names must be unique."
                raise ValueError(msg)

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
        self, pipeline: Pipeline, path: str | Path | None = None
    ) -> None:
        """Compile the pipeline to a JSON file.

        Args:
            pipeline (Pipeline): Pipeline to be compiled.
            path (str | Path | None, optional): Destination path.
                If none, it will be equal to `./<pipeline-name>.json`.
                Defaults to None.
        """
        graph = pipeline.compile()
        graph.name = pipeline.fn.__name__
        graph.creation_dt = datetime.now()

        if path is not None:
            path = Path(path)
        else:
            path = Path().resolve() / f"{pipeline.fn.__name__}.json"

        graph_json = graph.model_dump_json(indent=4, warnings="none")
        with path.open("w") as f:
            f.write(graph_json)

    def get_graph(self) -> Graph:
        """Get the compiled pipeline graph.

        Returns:
            Graph: Compiled pipeline graph.
        """
        if self.current is None:
            msg = "Target DAG not set."
            raise RuntimeError(msg)

        graph = self.current.get_graph()
        self.current = None if not self.dagstack else self.dagstack.pop()
        if self.current is not None:
            self.current.join(graph)
        return graph

    def If(self, expr: Node) -> IfTask:  # noqa: N802
        """If node.

        If the task passed evaluates to True, than the branch within
            the context manager will be evaluated.

        Args:
            expr (Node): Task node. It's output must be a Boolean.

        Returns:
            IfTask: If task.
        """
        if self.current is None:
            msg = "Target DAG not set."
            raise RuntimeError(msg)

        node = Node(uid=self.get_uid(), behavior=IfNode(type="IfNode"))
        self.current.register(node)
        expr.add_edge(node)
        return IfTask(
            node=node,
            pop_stack=self.pop_branchstack,
            push_stack=self.push_branchstack,
        )

    def Else(self) -> IfTask:  # noqa: N802
        """Else branch.

        Must be used after an If branch.

        Returns:
            IfTask: Else task.
        """
        if self.current is None:
            msg = "Target DAG not set."
            raise RuntimeError(msg)

        if self.current.last_branch is None:
            msg = "No current active branch."
            raise RuntimeError(msg)

        if self.current.last_branch.behavior.to_be_dropped:
            msg = "Calling else before if"
            raise ValueError(msg)

        if self.current.last_branch.behavior.active:
            msg = "Calling else within another branch"
            raise ValueError(msg)

        ifnode = self.current.last_branch
        ifnode.behavior.branch = False
        return IfTask(
            node=ifnode,
            pop_stack=self.pop_branchstack,
            push_stack=self.push_branchstack,
        )

    def Elif(self, expr: Node) -> IfTask:  # noqa: N802
        """Elif node.

        Args:
            expr (Node): Task node. It's output must be Boolean.

        Returns:
            IfTask: Task.
        """
        if self.current is None:
            msg = "Target DAG not set."
            raise RuntimeError(msg)

        self.current.register_elif_expression(expr)
        return self.If(expr)

    def OneOf(self, *args: Node) -> Node[OneOfNode]:  # noqa: N802
        """OneOf task.

        It has two major use cases:
        1. Selecting nodes from mutually excluded branches.
        2. Selecting The first successfully completed parent.


        Returns:
            Node[OneOfNode]: OneOf node.
        """
        if self.current is None:
            msg = "Target DAG not set."
            raise RuntimeError(msg)

        node = Node(uid=self.get_uid(), behavior=OneOfNode(type="OneOfNode"))
        self.current.register(node)
        for parent in args:
            parent.add_edge(node)
        return node

    def register(self, node: Node) -> None:
        """Register a node to the current DAG.

        Args:
            node (Node): Node to be registered.
        """
        if self.current is None:
            msg = "Target DAG not set."
            raise RuntimeError(msg)

        self.current.register(node)

    def push_branchstack(self, node: Node[IfNode]) -> None:
        """Push a branch to the stack.

        Args:
            node (Node[IfNode]): If node to be pushed.
        """
        if self.current is None:
            msg = "Target DAG not set."
            raise RuntimeError(msg)

        self.current.push_stack(node)

    def pop_branchstack(self) -> None:
        """Pop an If node from the stack."""
        if self.current is None:
            msg = "Target DAG not set."
            raise RuntimeError(msg)

        self.current.pop_stack()
