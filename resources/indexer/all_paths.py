"""Recursively get all paths in given directory and split into chunks for indexer_make_dag.py jobs."""

import argparse
import os
import subprocess
from datetime import datetime as dt


def check_call_print(cmd, cwd='.', shell=False):
    """Wrap subprocess.check_call and print command."""
    if shell and isinstance(cmd, list):
        raise Exception('Do not set shell=True and pass a list--pass a string.')
    print(f'Execute: {cmd} @ {cwd}')
    subprocess.check_call(cmd, cwd=cwd, shell=shell)


def _get_data_exp_paths_files(staging_dir, paths_root, workers, previous, paths_per_file=10000):
    output_root = os.path.join(staging_dir, 'indexer-data-exp/')
    file_orig = os.path.join(output_root, 'paths.orig')
    file_log = os.path.join(output_root, 'paths.log')
    file_sort = os.path.join(output_root, 'paths.sort')
    dir_split = os.path.join(output_root, 'paths/')

    if not os.path.exists(output_root):
        check_call_print(f'mkdir {output_root}'.split())

        # Get all file-paths in paths_root and sort the list
        check_call_print(f'python directory_scanner.py {paths_root} --workers {workers} > {file_orig} 2> {file_log}', shell=True)
        check_call_print(f'''sed -i '/^[[:space:]]*$/d' {file_orig}''', shell=True)  # remove blanks
        check_call_print(f'sort -T {output_root} {file_orig} > {file_sort}', shell=True)
        check_call_print(f'rm {file_orig}'.split())  # Cleanup

        # Get lines(file paths) unique to this scan versus the previous file
        if previous:
            check_call_print(f'comm -1 -3 {previous} {file_sort} > {file_sort}.unique', shell=True)
            check_call_print(f'mv {file_sort}.unique {file_sort}'.split())

        # split the file into n files
        check_call_print(f'mkdir {dir_split}'.split())
        check_call_print(f'split -l{paths_per_file} {file_sort} paths_file_'.split(), cwd=dir_split)

        # Copy/Archive
        time = dt.now().isoformat(timespec='seconds')
        file_archive = os.path.join(staging_dir, f'data-exp-{time}')
        check_call_print(f'mv {file_sort} {file_archive}'.split())
        print(f'Archive File: at {file_archive}')

    else:
        print(f'Writing Bypassed: {output_root} already exists. Use preexisting files.')


def main():
    """Main."""
    parser = argparse.ArgumentParser()
    parser.add_argument('paths_root', help='root directory to recursively scan for files.')
    parser.add_argument('--staging-dir', dest='staging_dir', required=True,
                        help='the base directory to store files for jobs, eg: /data/user/eevans/')
    parser.add_argument('--previous-all-paths', dest='previous_all_paths',
                        help='prior file with file paths, eg: /data/user/eevans/data-exp-2020-03-10T15:11:42.'
                        ' These files will be skipped.')
    parser.add_argument('--workers', type=int, help='max number of workers', required=True)
    args = parser.parse_args()

    for arg, val in vars(args).items():
        print(f'{arg}: {val}')

    # check paths in args
    for path in [args.paths_root, args.previous_all_paths]:
        if path and not os.path.exists(path):
            raise FileNotFoundError(path)

    _get_data_exp_paths_files(args.staging_dir, args.paths_root,
                              args.workers, args.previous_all_paths)


if __name__ == '__main__':
    main()
