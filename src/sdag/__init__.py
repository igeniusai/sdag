import sys

import sscheduler

from sdag.sdag import SDAG

__all__ = ["SDAG"]


def run_scheduler():
    """Start the Rust scheduler."""
    sscheduler.sscheduler_start(argv=sys.argv)
