# l2_indexer_make_dag.py
"""Make the Condor/DAGMan script for indexing /data/exp/."""

import argparse
import getpass
import os
import re
import subprocess

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


def _get_paths_files(paths_per_file=10000):
    root = os.path.join('/data/user/', getpass.getuser(), 'indexerall/')
    file_orig = os.path.join(root, 'paths.orig')
    dir_temp = os.path.join(root, 'pathstemp/')
    file_sort = os.path.join(root, 'paths.sort')
    dir_split = os.path.join(root, 'paths/')

    def check_call_print(cmd, cwd='.', shell=False):
        if shell and isinstance(cmd, []):
            raise Exception('Do not set shell=True and pass a list--pass a string.')
        print(f'{cmd} @ {cwd}')
        subprocess.check_call(cmd, cwd=cwd, shell=shell)

    if not os.path.exists(root):
        check_call_print(f'mkdir {root}'.split())

        # Get all file-paths in /data/exp/ and sort the list
        check_call_print(f'python directory_scanner.py /data/exp/ > {file_orig}', shell=True)
        check_call_print(f'sort -T {dir_temp} {file_orig} > {file_sort}', shell=True)

        # split the file into n files
        result = subprocess.run(f'wc -l {file_sort}'.split(), stdout=subprocess.PIPE)
        num = int(result.stdout.decode('utf-8').split()[0]) // paths_per_file
        check_call_print(f'mkdir {dir_split}'.split())
        check_call_print(f'split -n{num} {file_sort} paths_file_'.split(), cwd=dir_split)

    else:
        print(f'{root} already exists. Using preexisting files.')

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
    parser.add_argument('--dryrun', default=False, action='store_true',
                        help='does everything except submitting the condor job(s)')
    args = parser.parse_args()

    # make condor scratch directory
    scratch = os.path.join('/scratch/', getpass.getuser(), f'{args.level}indexer')
    if not os.path.exists(scratch):
        os.makedirs(scratch)

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

    # make condor file
    condorpath = os.path.join(scratch, 'condor')
    with open(condorpath, 'w') as file:
        file.write(f"""executable = {os.path.abspath(args.env)}
arguments = python indexer.py -s WIPAC {path_arg} -t {args.token} --timeout {args.timeout} --retries {args.retries} {blacklist_arg}
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
    with open(dagpath, 'w') as file:
        if args.level == 'Everything':
            paths = _get_paths_files()
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
    cmd = ['condor_submit_dag', '-maxjobs', str(args.maxjobs), dagpath]
    print(cmd)
    if args.dryrun:
        print('Indexer Aborted: Condor jobs not submitted.')
    else:
        subprocess.check_call(cmd, cwd=scratch)


if __name__ == '__main__':
    main()
