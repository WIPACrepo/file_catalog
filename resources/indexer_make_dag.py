# l2_indexer_make_dag.py
"""Make the Condor/DAGMan script for Level2 data."""

import argparse
import os
import re
import subprocess

LEVELS = {
    "L2": "filtered/level2",
    "L2P2": "filtered/level2pass2",
    "PFFilt": "filtered/PFFilt",
    "PFDST": "unbiased/PFDST",
    "PFRaw": "unbiased/PFRaw",
}


def _get_dirpaths(begin_year, end_year, level, root_dir):
    years = [str(y) for y in range(begin_year, end_year)]
    dirs = [d for d in os.scandir(os.path.abspath(root_dir)) if d.name in years]  # Ex: [/mnt/lfs6/exp/IceCube/2018, ...]

    days = []
    for _dir in dirs:
        path = os.path.join(_dir.path, LEVELS[level])  # Ex: /mnt/lfs6/exp/IceCube/2018/filtered/PFFilt
        try:
            # Ex: /mnt/lfs6/exp/IceCube/2018/filtered/PFFilt/0806
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
    parser.add_argument('--begin', type=int, help='beginning year in /data/exp/IceCube/', required=True)
    parser.add_argument('--end', type=int, help='end year in /data/exp/IceCube/', required=True)
    parser.add_argument('--level', help='processing level', choices=LEVELS.keys(), required=True)
    parser.add_argument('--rootdir', help='root directory path', default='/mnt/lfs6/exp/IceCube')
    parser.add_argument('--cpus', type=int, help='number of CPUs', default=2)
    parser.add_argument('--memory', type=int, help='amount of memory (MB)', default=2000)
    args = parser.parse_args()

    scratch = f"/scratch/eevans/{args.level}indexer"
    if not os.path.exists(scratch):
        os.makedirs(scratch)

    indexer_script = 'indexer.py'

    condorpath = os.path.join(scratch, 'condor')
    with open(condorpath, 'w') as f:
        f.write(f"""executable = {os.path.abspath(args.env)}
arguments = python {indexer_script} -s WIPAC $(PATH) -t {args.token} --timeout {args.timeout} --retries {args.retries}
output = {scratch}/$(JOBNUM).out
error = {scratch}/$(JOBNUM).err
log = {scratch}/$(JOBNUM).log
+FileSystemDomain = "blah"
should_transfer_files = YES
transfer_input_files = {os.path.abspath(indexer_script)}
request_cpus = {args.cpus}
request_memory = {args.memory}
notification = Error
queue
""")

    dagpath = os.path.join(scratch, 'dag')
    with open(dagpath, 'w') as f:
        for i, path in enumerate(_get_dirpaths(args.begin, args.end, args.level, args.rootdir)):
            f.write(f'JOB job{i} condor\n')
            f.write(f'VARS job{i} PATH="{path}"\n')
            f.write(f'VARS job{i} JOBNUM="{i}"\n')

    cmd = ['condor_submit_dag', '-maxjobs', str(args.maxjobs), dagpath]
    print(cmd)
    subprocess.check_call(cmd, cwd=scratch)


if __name__ == '__main__':
    main()
