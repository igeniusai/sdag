import sys

import sscheduler

from sdag._version import __version__
from sdag.sdag import SDAG

__all__ = ["__version__", "SDAG"]


def run_scheduler():
    """Start the Rust scheduler."""
    sscheduler.sscheduler_start(argv=sys.argv)
