from pathlib import Path

import pytest
from sdag4.compiler import SDAG, DAGCompiler
from sdag4.exceptions import TaskNotUniqueError
from sdag4.models import BranchNode, RootNode, ScriptPath, TaskNode


class TestSDAG:
    @pytest.fixture
    def sdag(self) -> SDAG:
        return SDAG()

    def test_add_pipeline(self, sdag: SDAG) -> None:
        from sdag4.wrappers import Pipeline

        def foo(): ...

        pipeline = Pipeline(foo)
        sdag.add_pipeline(pipeline)
        assert "foo" in sdag.pipelines

    def test_add_task(self, sdag: SDAG) -> None:
        from sdag4.wrappers import Task

        def foo(): ...

        task = Task(
            foo,
            name="task",
            cmd="bash",
            mode="wrap",
            cache=False,
            cache_local=False,
            debug=False,
            retries=0,
            script=ScriptPath(path=Path("a/path")),
        )

        sdag.add_task(task)
        assert "foo" in sdag.tasks
        with pytest.raises(TaskNotUniqueError):
            sdag.add_task(task)


class TestDAGCompiler:
    @pytest.fixture
    def compiler(self) -> DAGCompiler:
        return DAGCompiler()

    def test_set_dag(self, compiler: DAGCompiler) -> None:
        root = RootNode(uid=0, pipeline_name="dag")
        compiler.set_dag(root)
        assert compiler.active is not None
        assert compiler.active.dag.meta.pipeline_name == "dag"
        assert compiler.active.dag.nodes[0].kind == "root"
        assert compiler.active.root.kind == "root"
        assert compiler.active.end.kind == "end"

    def test_get_uid(self, compiler: DAGCompiler) -> None:
        for i in range(10):
            assert compiler.get_uid() == i

    def test_reset(self, compiler: DAGCompiler) -> None:
        uid = compiler.get_uid()
        root = RootNode(uid=compiler.get_uid(), pipeline_name="dag")
        compiler.set_dag(root)
        root = RootNode(uid=compiler.get_uid(), pipeline_name="dag")
        compiler.set_dag(root)
        compiler.reset()

        assert not compiler.outer
        assert compiler.active is None
        assert compiler.get_uid() == uid

    def test_get_dag(self, compiler: DAGCompiler) -> None:
        task = TaskNode(
            uid=5,
            name="task",
            fn_name="task",
            cache=False,
            cache_local=False,
            debug=False,
            mode="wrap",
            cmd="bash",
            retries=0,
            script=ScriptPath(path=Path()),
        )
        compiler.reset()
        root = RootNode(uid=compiler.get_uid(), pipeline_name="dag")
        compiler.set_dag(root)
        compiler.register(task)
        dag, root, end = compiler.get_dag()

        assert task.parents[0].uid == root.uid
        assert end.parents[0].uid == task.uid
        assert compiler.active is None
        assert len(dag.nodes) == 3

    def test_get_dag_with_outer(self, compiler: DAGCompiler) -> None:
        task = TaskNode(
            uid=5,
            name="task",
            fn_name="task",
            cache=False,
            cache_local=False,
            debug=False,
            mode="wrap",
            cmd="bash",
            retries=0,
            script=ScriptPath(path=Path()),
        )
        compiler.reset()
        root = RootNode(uid=compiler.get_uid(), pipeline_name="dag1")
        compiler.set_dag(root)
        root = RootNode(uid=compiler.get_uid(), pipeline_name="dag2")
        compiler.set_dag(root)
        compiler.register(task)
        inner_dag, inner_root, inner_end = compiler.get_dag()

        assert task.parents[0].uid == inner_root.uid
        assert inner_end.parents[0].uid == task.uid
        assert compiler.active is not None
        assert len(inner_dag.nodes) == 3

        dag, root, end = compiler.get_dag()
        assert inner_root.parents[0].uid == root.uid
        assert end.parents[0].uid == inner_end.uid
        assert len(dag.nodes) == 5

    def test_register_branch(self, compiler: DAGCompiler) -> None:
        root = RootNode(uid=compiler.get_uid(), pipeline_name="dag")
        compiler.set_dag(root)
        branch = BranchNode(uid=3, in_context=True)
        compiler.register_branch(branch)

        assert compiler.active is not None
        assert compiler.active.branches[-1] is branch

    def test_add_branch_edge(self, compiler: DAGCompiler) -> None:
        root = RootNode(uid=compiler.get_uid(), pipeline_name="dag")
        compiler.set_dag(root)
        branch = BranchNode(uid=3, in_context=True)
        compiler.register_branch(branch)
        node = RootNode(uid=5)
        compiler.add_branch_edge(node)

        assert node.parents[0].uid == branch.uid
        assert node.parents[0].kind.kind == "branch"
        assert node.parents[0].kind.branch
