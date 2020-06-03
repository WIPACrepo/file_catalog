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
    "PFRaw": "unbiased/PFRaw"
}
BEGIN_YEAR = 2005
END_YEAR = 2021


def _scan_dir_of_paths_files(dir_of_paths_files):
    return sorted([os.path.abspath(p.path) for p in os.scandir(dir_of_paths_files)])


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
    """Main."""
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--token', help='Auth Token', required=True)
    parser.add_argument('-j', '--maxjobs', default=500, help='max concurrent jobs')
    parser.add_argument('--timeout', type=int, default=300, help='REST client timeout duration')
    parser.add_argument('--retries', type=int, default=10, help='REST client number of retries')
    parser.add_argument('--level', help='shortcut to only index files from a specified processing level',
                        choices=LEVELS.keys())
    parser.add_argument('--levelyears', nargs=2, type=int, default=[BEGIN_YEAR, END_YEAR],
                        help='beginning and end year in /data/exp/IceCube/')
    parser.add_argument('--cpus', type=int, help='number of CPUs', default=2)
    parser.add_argument('--memory', type=int, help='amount of memory (MB)', default=2000)
    parser.add_argument('--dir-of-paths-files', dest='dir_of_paths_files',
                        help='the directory containing files, each of which contains a list of '
                        'filepaths to index. Ex: /data/user/eevans/indexer-data-exp/paths/')
    parser.add_argument('--blacklist', help='blacklist file containing all paths to skip')
    parser.add_argument('--dryrun', default=False, action='store_true',
                        help='does everything except submitting the condor job(s)')

    args = parser.parse_args()

    # check if either --level or --dir-of-paths-files
    if (args.level and args.dir_of_paths_files) or (not args.level and not args.dir_of_paths_files):
        raise Exception("Undefined action! Use either --level or --dir-of-paths-files, not both.")

    # check paths in args
    for f in [args.blacklist, args.dir_of_paths_files]:
        if f and not os.path.exists(f):
            raise FileNotFoundError(f)

    # make condor scratch directory
    if args.dir_of_paths_files:
        scratch = os.path.join('/scratch/', getpass.getuser(), 'Manual-indexer')
    elif args.level:
        scratch = os.path.join('/scratch/', getpass.getuser(), f'{args.level}-indexer')
    else:
        RuntimeError()
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
            if args.dir_of_paths_files:
                path_arg = '--paths-file $(PATHS_FILE)'
            elif args.level:
                path_arg = '$(PATH)'
            else:
                RuntimeError()

            # write
            file.write(f"""executable = {os.path.abspath('indexer_env.sh')}
arguments = python indexer.py -s WIPAC {path_arg} -t {args.token} --timeout {args.timeout} --retries {args.retries} {blacklist_arg} --log info --processes {args.cpus}
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
            if args.dir_of_paths_files:
                paths = _scan_dir_of_paths_files(args.dir_of_paths_files)
            elif args.level:
                begin_year = min(args.levelyears)
                end_year = max(args.levelyears)
                paths = _get_level_specific_dirpaths(begin_year, end_year, args.level)
            else:
                RuntimeError()

            for i, path in enumerate(paths):
                file.write(f'JOB job{i} condor\n')
                if args.dir_of_paths_files:
                    file.write(f'VARS job{i} PATHS_FILE="{path}"\n')
                elif args.level:
                    file.write(f'VARS job{i} PATH="{path}"\n')
                else:
                    RuntimeError()
                file.write(f'VARS job{i} JOBNUM="{i}"\n')

    # Execute
    if args.dryrun:
        print('Indexer Aborted: Condor jobs not submitted.')
    else:
        cmd = f'condor_submit_dag -maxjobs {args.maxjobs} {dagpath}'
        print(cmd)
        subprocess.check_call(cmd.split(), cwd=scratch)


if __name__ == '__main__':
    main()
