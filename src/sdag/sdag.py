from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from sdag.io import IOManager
from sdag.models import EndNode, Graph, IfNode, Node, OneOfNode, RootNode
from sdag.tasks import IfTask, Pipeline, Task


class DAG:
    def __init__(self, uid: str, name: str = ""):
        root_uid = f"_{name}_root_{uid}"
        end_uid = f"_{name}_end_{uid}"

        self.root = Node(uid=root_uid, behavior=RootNode(type="RootNode"))
        self.end = Node(uid=end_uid, behavior=EndNode(type="EndNode"))
        self.graph = Graph()
        self.branchstack: list[Node[IfNode]] = []
        self.last_branch: Node[IfNode] | None = None

    def add_root(self) -> None:
        for node in self.graph.nodes:
            if not node.parents:
                self.root.add_edge(node)
        self.graph.nodes.append(self.root)

    def add_end(self) -> None:
        for node in self.graph.nodes:
            if node.is_leaf():
                node.add_edge(self.end)
        self.graph.nodes.append(self.end)

    def register(self, node: Node) -> None:
        if self.last_branch is not None:
            if self.last_branch.behavior.active:
                self.last_branch.add_edge(node)
            elif self.last_branch.behavior.to_be_dropped:
                self.last_branch = None
            else:
                self.last_branch.behavior.to_be_dropped = True

        self.graph.nodes.append(node)

    def register_elif_expression(self, expr: Node) -> None:
        self.last_branch.behavior.branch = False
        self.last_branch.add_edge(expr)

    def pop_stack(self) -> None:
        self.last_branch = self.branchstack.pop()

    def push_stack(self, node: Node[IfNode]) -> None:
        self.last_branch = node
        self.branchstack.append(node)

    def validate_elif(self) -> None:
        if (
            not isinstance(self.last_branch.behavior, IfNode)
            or self.last_branch.behavior.active
        ):
            raise ValueError("Calling Elif before if")
        if not self.last_branch.behavior.to_be_dropped:
            raise ValueError("Conditional statement must be a task")
        if not self.last_branch.behavior.branch:
            raise ValueError("Calling Elif after Else")

    def validate_else(self) -> None:
        if (
            not isinstance(self.last_branch.behavior, IfNode)
            or self.last_branch.behavior.to_be_dropped
        ):
            raise ValueError("Calling else before if")
        if self.last_branch.behavior.active:
            raise ValueError("Calling else within another branch")

    def get_graph(self) -> Graph:
        self.add_root()
        self.add_end()
        return self.graph

    def join(self, graph: Graph) -> None:
        self.graph.nodes.extend(graph.nodes)


class SDAG:
    def __init__(self):
        self.uid = 0
        self.taskdict: dict[str, Callable] = {}
        self.dagstack: list[DAG] = []
        self.current: DAG | None = None

    def run(self) -> None:
        manager = IOManager()
        node = manager.find_node_to_be_executed()
        fn = self.taskdict[node.behavior.fname]
        input_kwargs = manager.get_input(node, fn)
        output = fn(**input_kwargs)
        manager.serialize_output(output, node.uid)

    def set_current_dag(self, name: str) -> None:
        if self.current is not None:
            self.dagstack.append(self.current)
        self.current = DAG(uid=self.get_uid(), name=name)

    def task(
        self, launch_script: str | Path, caching: bool = False, retries: int = 0
    ) -> Callable[[Callable], Task]:
        def return_task(fn: Callable) -> Task:
            if fn.__name__ in self.taskdict:
                raise ValueError("Task names must be unique.")

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
        uid = self.uid
        self.uid += 1
        return str(uid)

    def pipeline(self, fn: Callable[[], None]) -> Pipeline:
        return Pipeline(fn=fn, set_dag=self.set_current_dag, get_graph=self.get_graph)

    def compile(self, pipeline: Pipeline, path: str | Path | None = None) -> DAG:
        graph = pipeline.compile()
        graph.name = pipeline.fn.__name__
        graph.creation_dt = datetime.now()

        if path is not None:
            path = Path(path)
        else:
            path = Path(".").resolve() / f"{pipeline.fn.__name__}.json"

        graph_json = graph.model_dump_json(indent=4, warnings="none")
        with path.open("w") as f:
            f.write(graph_json)

    def get_graph(self) -> Graph:
        graph = self.current.get_graph()
        self.current = None if not self.dagstack else self.dagstack.pop()
        if self.current is not None:
            self.current.join(graph)
        return graph

    def If(self, expr: Node):
        node = Node(uid=self.get_uid(), behavior=IfNode(type="IfNode"))
        self.current.register(node)
        expr.add_edge(node)
        return IfTask(
            node=node,
            pop_stack=self.pop_branchstack,
            push_stack=self.push_branchstack,
        )

    def Else(self) -> IfTask:
        self.current.validate_else()
        ifnode = self.current.last_branch
        ifnode.behavior.branch = False
        return IfTask(
            node=ifnode,
            pop_stack=self.pop_branchstack,
            push_stack=self.push_branchstack,
        )

    def Elif(self, expr: Node) -> IfTask:
        self.current.validate_elif()
        self.current.register_elif_expression(expr)
        return self.If(expr)

    def OneOf(self, *args: Node) -> Node[OneOfNode]:
        node = Node(uid=self.get_uid(), behavior=OneOfNode(type="OneOfNode"))
        self.current.register(node)
        for parent in args:
            parent.add_edge(node)
        return node

    def register(self, node: Node) -> None:
        self.current.register(node)

    def push_branchstack(self, node: Node[IfNode]) -> None:
        self.current.push_stack(node)

    def pop_branchstack(self) -> None:
        self.current.pop_stack()
