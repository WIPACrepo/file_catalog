# l2_indexer_make_dag.py
"""Make the Condor/DAGMan script for Level2 data."""

import argparse
import os
import re
import subprocess


def _get_dirpaths():
    years = [str(y) for y in range(2007, 2021)]
    dirs = [d for d in os.scandir(os.path.abspath('/mnt/lfs6/exp/IceCube')) if d.name in years]

    days = []
    for _dir in dirs:
        path = os.path.join(_dir.path, "filtered/level2")
        try:
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
    args = parser.parse_args()

    scratch = "/scratch/eevans/l2indexer"
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
notification = Error
queue
""")
        # request_memory = 2500

    dagpath = os.path.join(scratch, 'dag')
    with open(dagpath, 'w') as f:
        for i, path in enumerate(_get_dirpaths()):
            f.write(f'JOB job{i} condor\n')
            f.write(f'VARS job{i} PATH="{path}"\n')
            f.write(f'VARS job{i} JOBNUM="{i}"\n')

    cmd = ['condor_submit_dag', '-maxjobs', str(args.maxjobs), dagpath]
    print(cmd)
    subprocess.check_call(cmd, cwd=scratch)


if __name__ == '__main__':
    main()
