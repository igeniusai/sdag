"""CLI entry point."""

from sdag.parser import ParserBuilder


def main():
    """CLI entry point."""
    parser = (
        ParserBuilder()
        .add_version()
        .add_compile_subparser()
        .add_run_subparser()
        .add_compile_run_subparser()
        .add_kill_subparser()
        .add_prune_subparser()
        .get_parser()
    )

    args = parser.parse_args()
    args.fn(args)
