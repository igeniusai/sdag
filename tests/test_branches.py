"""Large tests to verify branch correcteness."""

import pytest

from sdag import SDAG
from sdag.exceptions import SDAGError
from sdag.models import BranchType, IfNode, LogicalType, Parent, TaskNode


def test_elif_without_if() -> None:
    """Elif called without a preceding If."""
    sdag = SDAG()

    @sdag.task(launch_script="/")
    def stage() -> None: ...

    @sdag.task(launch_script="/")
    def condition() -> bool:
        return True

    @sdag.pipeline
    def pipeline():
        with sdag.Elif(condition()):
            stage()

    with pytest.raises(SDAGError):
        sdag.compile(pipeline)


def test_elif_within_if() -> None:
    """Call Elif within another branch but without If."""
    sdag = SDAG()

    @sdag.task(launch_script="/")
    def stage() -> None: ...

    @sdag.task(launch_script="/")
    def condition() -> bool:
        return True

    @sdag.pipeline
    def pipeline():
        with sdag.If(condition()):  # noqa: SIM117
            with sdag.Elif(condition()):
                stage()

    with pytest.raises(SDAGError):
        sdag.compile(pipeline)


def test_else_within_if() -> None:
    """Call Else within an If."""
    sdag = SDAG()

    @sdag.task(launch_script="/")
    def stage() -> None: ...

    @sdag.task(launch_script="/")
    def condition() -> bool:
        return True

    @sdag.pipeline
    def pipeline():
        with sdag.If(condition()):  # noqa: SIM117
            with sdag.Else():
                stage()

    with pytest.raises(SDAGError):
        sdag.compile(pipeline)


def test_else_without_if() -> None:
    """Call Else without a preceding If."""
    sdag = SDAG()

    @sdag.task(launch_script="/")
    def stage() -> None: ...

    @sdag.pipeline
    def pipeline():
        with sdag.Else():
            stage()

    with pytest.raises(SDAGError):
        sdag.compile(pipeline)


def test_elif_after_else() -> None:
    """Call Elif after Else."""
    sdag = SDAG()

    @sdag.task(launch_script="/")
    def stage() -> None: ...

    @sdag.task(launch_script="/")
    def condition() -> bool:
        return True

    @sdag.pipeline
    def pipeline():
        with sdag.If(condition()):
            stage()
        with sdag.Else():
            stage()
        with sdag.Elif(condition()):
            stage()

    with pytest.raises(SDAGError):
        sdag.compile(pipeline)


def test_if() -> None:
    """Check If syntax relationships."""
    sdag = SDAG()

    @sdag.task(launch_script="/")
    def stage() -> None: ...

    @sdag.task(launch_script="/")
    def condition() -> bool:
        return True

    @sdag.pipeline
    def pipeline():
        with sdag.If(condition()):
            stage()

    graph = pipeline.compile()
    ifnode = next(n for n in graph.nodes if n.behavior.type == "IfNode")
    cond = next(
        n
        for n in graph.nodes
        if n.behavior.type == "TaskNode" and n.behavior.fname == "condition"
    )
    func = next(
        n
        for n in graph.nodes
        if n.behavior.type == "TaskNode" and n.behavior.fname == "stage"
    )

    assert isinstance(ifnode.behavior, IfNode)
    assert isinstance(cond.behavior, TaskNode)

    assert ifnode.parents == [Parent(uid=cond.uid, parent_type=LogicalType())]
    assert func.parents == [
        Parent(uid=ifnode.uid, parent_type=BranchType(branch=True))
    ]


def test_if_elif_else() -> None:
    """Complete test of If/Elif/Else relationships."""
    sdag = SDAG()

    @sdag.task(launch_script="/")
    def task_if() -> None: ...

    @sdag.task(launch_script="/")
    def task_elif() -> None: ...

    @sdag.task(launch_script="/")
    def task_else() -> None: ...

    @sdag.task(launch_script="/")
    def cond_if() -> bool:
        return False

    @sdag.task(launch_script="/")
    def cond_elif() -> bool:
        return False

    @sdag.pipeline
    def pipeline():
        with sdag.If(cond_if()):
            task_if()
        with sdag.Elif(cond_elif()):
            task_elif()
        with sdag.Else():
            task_else()

    graph = pipeline.compile()

    ifnodes = (n for n in graph.nodes if n.behavior.type == "IfNode")
    nodeif = next(ifnodes)
    nodeelif = next(ifnodes)

    condif = next(
        n
        for n in graph.nodes
        if n.behavior.type == "TaskNode" and n.behavior.fname == "cond_if"
    )

    condelif = next(
        n
        for n in graph.nodes
        if n.behavior.type == "TaskNode" and n.behavior.fname == "cond_elif"
    )

    taskif = next(
        n
        for n in graph.nodes
        if n.behavior.type == "TaskNode" and n.behavior.fname == "task_if"
    )

    taskelif = next(
        n
        for n in graph.nodes
        if n.behavior.type == "TaskNode" and n.behavior.fname == "task_elif"
    )

    taskelse = next(
        n
        for n in graph.nodes
        if n.behavior.type == "TaskNode" and n.behavior.fname == "task_else"
    )

    assert isinstance(nodeif.behavior, IfNode)
    assert isinstance(nodeelif.behavior, IfNode)
    assert isinstance(condif.behavior, TaskNode)
    assert isinstance(condelif.behavior, TaskNode)
    assert isinstance(taskif.behavior, TaskNode)
    assert isinstance(taskelif.behavior, TaskNode)
    assert isinstance(taskelse.behavior, TaskNode)

    assert condif.parents
    assert nodeif.parents == [
        Parent(uid=condif.uid, parent_type=LogicalType())
    ]
    assert condelif.parents == [
        Parent(uid=nodeif.uid, parent_type=BranchType(branch=False))
    ]
    assert nodeelif.parents == [
        Parent(uid=condelif.uid, parent_type=LogicalType())
    ]
    assert taskelif.parents == [
        Parent(uid=nodeelif.uid, parent_type=BranchType(branch=True))
    ]
    assert taskelse.parents == [
        Parent(uid=nodeelif.uid, parent_type=BranchType(branch=False))
    ]
