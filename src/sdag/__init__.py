"""Library to build and schedule pipelines."""

import sys

import sscheduler

from sdag._version import __version__
from sdag.dags import SDAG
from sdag.models import Artifact
from sdag.sdag import sdag

__all__ = ["SDAG", "Artifact", "__version__", "sdag", "sscheduler"]


def run_scheduler():
    """Start the Rust scheduler."""
    sscheduler.sscheduler_start(argv=sys.argv)  # type: ignore
