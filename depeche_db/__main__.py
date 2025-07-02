import argparse as _argparse
from ._aggregated_stream import AggregatedStream


def main():
    """
    Commands:
        - aggregated_stream_migration: Shows the migration script for the aggregated stream.
    """
    parser = _argparse.ArgumentParser(description="DepecheDB CLI tools")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Add aggregated_stream_migration subcommand
    migration_parser = subparsers.add_parser(
        "aggregated_stream_migration",
        help="Shows the migration script for the aggregated stream"
    )
    migration_parser.add_argument(
        "message_store_name",
        help="Name of the message store"
    )
    migration_parser.add_argument(
        "aggregated_stream_name", 
        help="Name of the aggregated stream"
    )
    
    args = parser.parse_args()
    
    if args.command == "aggregated_stream_migration":
        migration_ddl = AggregatedStream.get_migration_ddl_0_11_0(
            args.message_store_name,
            args.aggregated_stream_name
        )
        print(migration_ddl)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
