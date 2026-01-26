"""Library to build and schedule pipelines."""

import sscheduler

from sdag._version import __version__
from sdag.dags import SDAG
from sdag.models import Artifact
from sdag.sdag import sdag
from sdag.viewer import DAGViewer

__all__ = [
    "SDAG",
    "Artifact",
    "DAGViewer",
    "__version__",
    "sdag",
    "sscheduler",
]
