import argparse as _argparse

from ._aggregated_stream import AggregatedStream


def main():
    """
    Commands:
        - generate-migration-script: Generate migration scripts for aggregated streams.
        - aggregated_stream_migration: Shows the migration script for the aggregated stream (deprecated).
    """
    parser = _argparse.ArgumentParser(description="DepecheDB CLI tools")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Add generate-migration-script subcommand
    migration_parser = subparsers.add_parser(
        "generate-migration-script",
        help="Generate migration scripts for aggregated streams",
    )
    migration_parser.add_argument(
        "prev_version", help="Previous version (e.g., '0.8')"
    )
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
        help="Name of an aggregated stream (can be specified multiple times)"
    )

    # Keep old subcommand for backward compatibility
    old_migration_parser = subparsers.add_parser(
        "aggregated_stream_migration",
        help="Shows the migration script for the aggregated stream (deprecated)",
    )
    old_migration_parser.add_argument(
        "message_store_name", help="Name of the message store"
    )
    old_migration_parser.add_argument(
        "aggregated_stream_name", help="Name of the aggregated stream"
    )

    args = parser.parse_args()

    if args.command == "generate-migration-script":
        migration_generators = AggregatedStream.migration_script_generators()
        
        if args.target_version not in migration_generators:
            print(f"Error: No migration available for target version {args.target_version}")
            print(f"Available versions: {', '.join(migration_generators.keys())}")
            return
        
        generators = migration_generators[args.target_version]
        
        for stream_name in args.aggregated_streams:
            print(f"-- Migration for aggregated stream: {stream_name}")
            for generator in generators:
                migration_ddl = generator(stream_name, args.message_store)
                print(migration_ddl)
            print()
    
    elif args.command == "aggregated_stream_migration":
        migration_ddl = AggregatedStream.get_migration_ddl_0_11_0(
            message_store_name=args.message_store_name,
            aggregated_stream_name=args.aggregated_stream_name,
        )
        print(migration_ddl)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
