"""Types."""

from typing import Literal

ExecMode = Literal["wrap", "ext"]
"""Available execution modes."""

Commands = Literal["bash", "sbatch"]
"""Available commands."""

Scope = Literal["local", "global"]
"""Task scopes."""
