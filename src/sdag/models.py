from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Annotated, Generic, Literal, Self, TypeVar

from pydantic import BaseModel, Field

StrPath = str | Path


class BaseNodeType(ABC, BaseModel):
    @abstractmethod
    def add_child(self, uid: str) -> None: ...

    @abstractmethod
    def is_leaf(self) -> bool: ...


class InputKwarg(BaseModel):
    key: str
    value: str


class TaskNode(BaseNodeType):
    type: Literal["TaskNode"]
    fname: str
    launch_script: Path
    caching: bool
    return_type: None | str = None
    input_kwargs: list[InputKwarg] = Field(default_factory=list)
    children: list[str] = Field(default_factory=list)

    def add_child(self, uid: str) -> None:
        self.children.append(uid)

    def is_leaf(self) -> bool:
        return not self.children


class IfNode(BaseNodeType):
    type: Literal["IfNode"]
    branch: bool = True
    true_branch: list[str] = Field(default_factory=list)
    false_branch: list[str] = Field(default_factory=list)
    active: bool = False
    to_be_dropped: bool = False

    def add_child(self, uid: str) -> None:
        if self.branch:
            self.true_branch.append(uid)
        else:
            self.false_branch.append(uid)

    def is_leaf(self) -> bool:
        return False


class OneOfNode(BaseNodeType):
    type: Literal["OneOfNode"]
    children: list[str] = Field(default_factory=list)

    def add_child(self, uid: str) -> None:
        self.children.append(uid)

    def is_leaf(self) -> bool:
        return not self.children


class EndNode(BaseNodeType):
    type: Literal["EndNode"]
    children: list[str] = Field(default_factory=list)

    def add_child(self, uid: str) -> None:
        self.children.append(uid)

    def is_leaf(self) -> bool:
        return not self.children


class RootNode(BaseNodeType):
    type: Literal["RootNode"]
    children: list[str] = Field(default_factory=list)

    def add_child(self, uid: str) -> None:
        self.children.append(uid)

    def is_leaf(self) -> bool:
        return not self.children


T = TypeVar("T", bound=BaseNodeType)


class Parent(BaseModel):
    uid: str
    name: str


class Node(BaseModel, Generic[T]):
    uid: str
    parents: list[Parent] = Field(default_factory=list)
    behavior: T

    def is_leaf(self) -> bool:
        return self.behavior.is_leaf()

    def add_edge(self, child: Self, name: str = "") -> None:
        child._add_parent(name, self)
        self._add_child(child)

    def _add_child(self, node: Self) -> None:
        return self.behavior.add_child(node.uid)

    def _add_parent(self, name: str, node: Self) -> None:
        parent = Parent(uid=node.uid, name=name)
        self.parents.append(parent)


BehaviorUnion = Annotated[
    TaskNode | IfNode | EndNode | RootNode | OneOfNode, Field(discriminator="type")
]

NodeUnion = Node[BehaviorUnion]


class Graph(BaseModel):
    name: str = ""
    creation_dt: datetime = Field(default_factory=datetime.now)
    nodes: list[NodeUnion] = Field(default_factory=list)

    def get_root(self) -> Node[RootNode]:
        for node in self.nodes:
            if not node.parents:
                return node

        raise ValueError("Root node not found")

    def get_end(self) -> Node[EndNode]:
        for node in self.nodes:
            if node.is_leaf():
                return node

        raise ValueError("End node not found")
