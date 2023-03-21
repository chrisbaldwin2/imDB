import argparse
from loader.sql_ingest import sql_ingest
from loader.loader import download_data, write_secrets

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--download', action='store_true')
    parser.add_argument('--ingest', action='store_true')
    parser.add_argument('--skip_load', action='store_true', help='Skip loading data')
    parser.add_argument('--clear_tables', action='store_false', help='Clear tables')
    parser.add_argument('--filter_data', action='store_false', help='Filter data')
    parser.add_argument('--num_lines', type=int, default=100_000, help='Number of lines to load')
    parser.add_argument('--user', type=str, help='The MySQL username to be stored')
    parser.add_argument('--password', type=str, help='The MySQL password to be stored')
    parser.add_argument('--database', type=str, default='imDB', help='The MySQL database to be used')
    parser.add_argument('--host', type=str, default='localhost', help='The MySQL host to be used')
    return parser.parse_args()

def main():
    args = parse_args()
    if args.user or args.password:
        if not (args.user and args.password):
            raise Exception("Must pass both user and password in a single command")
        write_secrets({'user': args.user, 'password': args.password, 'database': args.database, 'host': args.host})
    if args.download:
        download_data()
    if args.ingest:
        sql_ingest(args.skip_load, args.filter_data, args.clear_tables, args.num_lines)

if __name__ == '__main__':
    main()