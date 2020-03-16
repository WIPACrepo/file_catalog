# l2_indexer_make_dag.py
"""Make the Condor/DAGMan script for indexing /data/exp/."""

import argparse
import getpass
import os
import re
import subprocess
from datetime import datetime as dt

LEVELS = {
    "L2": "filtered/level2",
    "L2P2": "filtered/level2pass2",
    "PFFilt": "filtered/PFFilt",
    "PFDST": "unbiased/PFDST",
    "PFRaw": "unbiased/PFRaw",
    "Everything": None,
}
BEGIN_YEAR = 2005
END_YEAR = 2021


def check_call_print(cmd, cwd='.', shell=False):
    """Wrap subprocess.check_call and print command."""
    if shell and isinstance(cmd, list):
        raise Exception('Do not set shell=True and pass a list--pass a string.')
    print(f'Execute: {cmd} @ {cwd}')
    subprocess.check_call(cmd, cwd=cwd, shell=shell)


def _get_data_exp_paths_files(previous=None, paths_per_file=10000):
    root = os.path.join('/data/user/', getpass.getuser(), 'indexer-data-exp/')
    file_orig = os.path.join(root, 'paths.orig')
    file_sort = os.path.join(root, 'paths.sort')
    dir_split = os.path.join(root, 'paths/')

    if not os.path.exists(root):
        check_call_print(f'mkdir {root}'.split())

        # Get all file-paths in /data/exp/ and sort the list
        check_call_print(f'python directory_scanner.py /data/exp/ > {file_orig}', shell=True)
        check_call_print(f'''sed -i '/^[[:space:]]*$/d' {file_orig}''', shell=True)  # remove blanks
        check_call_print(f'sort -T {root} {file_orig} > {file_sort}', shell=True)
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
        file_archive = os.path.join('/data/user/', getpass.getuser(), f'data-exp-{time}')
        check_call_print(f'mv {file_sort} {file_archive}'.split())
        print(f'Archive File: at {file_archive}')

    else:
        print(f'Writing Bypassed: {root} already exists. Using preexisting files.')

    return sorted([os.path.abspath(p.path) for p in os.scandir(dir_split)])


def _get_level_specific_dirpaths(begin_year, end_year, level):
    years = [str(y) for y in range(begin_year, end_year)]

    # Ex: [/data/exp/IceCube/2018, ...]
    dirs = [d for d in os.scandir(os.path.abspath('/data/exp/IceCube')) if d.name in years]

    days = []
    for _dir in dirs:
        # Ex: /data/exp/IceCube/2018/filtered/PFFilt
        path = os.path.join(_dir.path, LEVELS[level])
        try:
            # Ex: /data/exp/IceCube/2018/filtered/PFFilt/0806
            day_dirs = [d.path for d in os.scandir(path) if re.match(r"\d{4}", d.name)]
            days.extend(day_dirs)
        except:
            pass

    return days


def main():
    """main"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', help='script to load env', required=True)
    parser.add_argument('-t', '--token', help='Auth Token', required=True)
    parser.add_argument('-j', '--maxjobs', default=500, help='max concurrent jobs')
    parser.add_argument('--timeout', type=int, default=300, help='REST client timeout duration')
    parser.add_argument('--retries', type=int, default=10, help='REST client number of retries')
    parser.add_argument('--level', help='processing level. `Everything` will index all of /data/exp/',
                        choices=LEVELS.keys(), required=True)
    parser.add_argument('--levelyears', nargs=2, type=int, default=[BEGIN_YEAR, END_YEAR],
                        help='beginning and end year in /data/exp/IceCube/')
    parser.add_argument('--cpus', type=int, help='number of CPUs', default=2)
    parser.add_argument('--memory', type=int, help='amount of memory (MB)', default=2000)
    parser.add_argument('--blacklist', help='blacklist file containing all paths to skip')
    parser.add_argument('--previous-data-exp', dest='previous_data_exp',
                        help='prior file with file paths, eg: /data/user/eevans/data-exp-2020-03-10T15:11:42.'
                        ' These files will be skipped.')
    parser.add_argument('--dryrun', default=False, action='store_true',
                        help='does everything except submitting the condor job(s)')
    args = parser.parse_args()

    # check paths in args
    for path in [args.env, args.blacklist, args.previous_data_exp]:
        if path and not os.path.exists(path):
            raise FileNotFoundError(path)

    # make condor scratch directory
    scratch = os.path.join('/scratch/', getpass.getuser(), f'{args.level}-indexer')
    if not os.path.exists(scratch):
        os.makedirs(scratch)

    # make condor file
    condorpath = os.path.join(scratch, 'condor')
    if os.path.exists(condorpath):
        print(f'Writing Bypassed: {condorpath} already exists. Using preexisting condor file.')
    else:
        with open(condorpath, 'w') as file:
            # configure transfer_input_files
            transfer_input_files = ['indexer.py']
            blacklist_arg = ''
            if args.blacklist:
                blacklist_arg = f'--blacklist {args.blacklist}'
                transfer_input_files.append(args.blacklist)

            # path or paths_file
            path_arg = ''
            if args.level == 'Everything':
                path_arg = '--paths-file $(PATHS_FILE)'
            else:
                path_arg = '$(PATH)'

            # write
            file.write(f"""executable = {os.path.abspath(args.env)}
arguments = python indexer.py -s WIPAC {path_arg} -t {args.token} --timeout {args.timeout} --retries {args.retries} {blacklist_arg} -l info
output = {scratch}/$(JOBNUM).out
error = {scratch}/$(JOBNUM).err
log = {scratch}/$(JOBNUM).log
+FileSystemDomain = "blah"
should_transfer_files = YES
transfer_input_files = {",".join([os.path.abspath(f) for f in transfer_input_files])}
request_cpus = {args.cpus}
request_memory = {args.memory}
notification = Error
queue
""")

    # make dag file
    dagpath = os.path.join(scratch, 'dag')
    if os.path.exists(dagpath):
        print(f'Writing Bypassed: {dagpath} already exists. Using preexisting dag file.')
    else:
        # write
        with open(dagpath, 'w') as file:
            if args.level == 'Everything':
                paths = _get_data_exp_paths_files(previous=args.previous_data_exp)
            else:
                begin_year = min(args.levelyears)
                end_year = max(args.levelyears)
                paths = _get_level_specific_dirpaths(begin_year, end_year, args.level)

            for i, path in enumerate(paths):
                file.write(f'JOB job{i} condor\n')
                if args.level == 'Everything':
                    file.write(f'VARS job{i} PATHS_FILE="{path}"\n')
                else:
                    file.write(f'VARS job{i} PATH="{path}"\n')
                file.write(f'VARS job{i} JOBNUM="{i}"\n')

    # Execute
    if args.dryrun:
        print('Indexer Aborted: Condor jobs not submitted.')
    else:
        check_call_print(f'condor_submit_dag -maxjobs {args.maxjobs} {dagpath}', cwd=scratch)


if __name__ == '__main__':
    main()
