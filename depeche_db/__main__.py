import argparse as _argparse
from typing import Tuple

from ._aggregated_stream import AggregatedStream


def main():
    """
    Commands:
        - generate-migration-script: Generate migration scripts for aggregated streams.
    """
    parser = _argparse.ArgumentParser(description="DepecheDB CLI tools")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Add generate-migration-script subcommand
    migration_parser = subparsers.add_parser(
        "generate-migration-script",
        help="Generate migration scripts for aggregated streams",
    )
    migration_parser.add_argument("prev_version", help="Previous version (e.g., '0.8')")
    migration_parser.add_argument(
        "target_version", help="Target version (e.g., '0.11')"
    )
    migration_parser.add_argument(
        "--message-store", required=True, help="Name of the message store"
    )
    migration_parser.add_argument(
        "--aggregated-stream",
        action="append",
        required=True,
        dest="aggregated_streams",
        help="Name of an aggregated stream (can be specified multiple times)",
    )

    args = parser.parse_args()

    if args.command == "generate-migration-script":
        migration_generators = AggregatedStream.migration_script_generators()
        prev_version = parse_version(args.prev_version)
        target_version = parse_version(args.target_version)

        for version, generators in migration_generators.items():
            if version < prev_version:
                continue
            if version > target_version:
                continue

            if generators:
                print("-- Migration for version:", ".".join(map(str, version)))

            for stream_name in args.aggregated_streams:
                print(f"-- Migration for aggregated stream: {stream_name}")
                for generator in generators:
                    migration_ddl = generator(stream_name, args.message_store)
                    print(migration_ddl)
                print()

    else:
        parser.print_help()


def parse_version(version_str: str) -> Tuple[int, int]:
    """
    Parse a version string into a tuple of integers.
    """
    major, minor, *_ = [int(part) for part in version_str.split(".")]
    return (major, minor)


if __name__ == "__main__":
    main()
