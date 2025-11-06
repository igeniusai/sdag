"""Check that Python is able to talk to the Rust scheduler."""

import sscheduler


def test_sscheduler_call() -> None:
    """Test the scheduler call from Python."""
    sscheduler.sscheduler_start(argv=["sdag", "-h"])  # type: ignore
