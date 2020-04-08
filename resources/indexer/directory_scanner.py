"""Recursively scan directory paths and print all file paths."""

import argparse
import logging
import os
import stat
from concurrent.futures import ProcessPoolExecutor
from time import sleep


def process_dir(path):
    """Print out file paths and return sub-directories."""
    try:
        scan = os.scandir(path)
    except (PermissionError, FileNotFoundError):
        scan = []
    dirs = []

    all_file_count = 0
    for dir_entry in scan:
        try:
            mode = os.lstat(dir_entry.path).st_mode
            if stat.S_ISLNK(mode) or stat.S_ISSOCK(mode) or stat.S_ISFIFO(mode) or stat.S_ISBLK(mode) or stat.S_ISCHR(mode):
                logging.info(f"Non-processable file: {dir_entry.path}")
                continue
        except PermissionError:
            logging.info(f"Permission denied: {dir_entry.path}")
            continue

        if dir_entry.is_dir():
            dirs.append(dir_entry.path)
        elif dir_entry.is_file():
            all_file_count = all_file_count + 1
            if not dir_entry.path.strip():
                logging.info(f"Blank file name in: {os.path.dirname(dir_entry.path)}")
            else:
                try:
                    print(dir_entry.path)
                except UnicodeEncodeError:
                    logging.info(f"Invalid file name in: {os.path.dirname(dir_entry.path)}")

    return dirs, all_file_count


def main():
    """Main."""
    parser = argparse.ArgumentParser(description='Find directories under PATH(s)',
                                     epilog='Notes: (1) symbolic links are never followed.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('path', metavar='PATH', nargs='+', help='path(s) to scan.')
    parser.add_argument('--workers', type=int, help='max number of workers', required=True)
    args = parser.parse_args()

    dirs = [os.path.abspath(p) for p in args.path]
    futures = []
    all_file_count = 0
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        while futures or dirs:
            for d in dirs:
                futures.append(pool.submit(process_dir, d))
            while not futures[0].done():
                sleep(0.1)
            future = futures.pop(0)
            dirs, result_all_file_count = future.result()
            all_file_count = all_file_count + result_all_file_count

    logging.info(f"File Count: {all_file_count}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
