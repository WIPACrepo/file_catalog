import argparse
import logging

from file_catalog.server import Server

def main():
    parser = argparse.ArgumentParser(description='File catalog')
    parser.add_argument('-p', '--port', help='port to listen on')
    parser.add_argument('--db_host', help='MongoDB host')
    parser.add_argument('--debug', action='store_true', default=False, help='Debug flag')
    args = parser.parse_args()
    kwargs = {k:v for k,v in vars(args).items() if v}
    logging.basicConfig(level=('DEBUG' if args.debug else 'INFO'))
    Server(**kwargs).run()

if __name__ == '__main__':
    main()