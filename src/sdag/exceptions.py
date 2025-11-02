"""Custom exceptions."""


class SDAGError(Exception):
    """Base exception, can be used to catch them all."""


class NodeNotFoundError(SDAGError):
    """Node not found in the compiled JSON."""

    def __init__(self, uid: str):
        """Raise the error.

        Args:
            uid (str): Unique id of the missing node.
        """
        msg = f"Node '{uid}' is not found in the compiled pipeline."
        super().__init__(msg)


class RootNotFoundError(SDAGError):
    """Root node not found."""

    def __init__(self):
        """Raise the exception."""
        msg = "Root node not found."
        super().__init__(msg)


class EndNotFoundError(SDAGError):
    """End node not found."""

    def __init__(self):
        """Raise the exception."""
        msg = "End node not found."
        super().__init__(msg)


class NotATaskError(SDAGError):
    """The node is not a task, so it cannot be executed."""

    def __init__(self, uid: str):
        """Raise the error.

        Args:
            uid (str): Unique id of the node that is not a task.
        """
        msg = f"Node {uid} is not a task!"
        super().__init__(msg)


class TaskNotUniqueError(SDAGError):
    """Task name is not unique."""

    def __init__(self, name: str):
        """Raise the error.

        Args:
            name (str): Not unique task name.
        """
        msg = f"Task name '{name}' is not unique."
        super().__init__(msg)


class DAGNotSetError(SDAGError):
    """Raised when registering stuff without a pipeline set.

    DAG can be None, so it must be checked everytime it is accessed.
    """

    def __init__(self):
        """Raise the exception."""
        msg = "Target DAG is not set."
        super().__init__(msg)


class MissingActiveBranchError(SDAGError):
    """Active branch is missing."""

    def __init__(self):
        """Raise the exception."""
        msg = "No current active branch."
        super().__init__(msg)


class IncorrectElseError(SDAGError):
    """Else is incorrectly used."""

    def __init__(self):
        """Raise the exception."""
        msg = "Else statement incorrectly used."
        super().__init__(msg)


class IncorrectElifError(SDAGError):
    """Elif is incorrectly used."""

    def __init__(self):
        """Raise the exception."""
        msg = "Elif statement incorrectly used."
        super().__init__(msg)
