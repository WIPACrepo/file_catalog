"""Recursively scan directory paths and print all file paths."""

import argparse
import logging
import os
import pathlib
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
        plp = pathlib.Path(dir_entry.path)
        if plp.is_symlink() or plp.is_socket() or plp.is_fifo() or plp.is_block_device() or plp.is_char_device():
            continue
        elif dir_entry.is_dir():
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
    args = parser.parse_args()

    dirs = [os.path.abspath(p) for p in args.path]
    futures = []
    all_file_count = 0
    with ProcessPoolExecutor() as pool:
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
