"""Make the Condor script for all_paths.py."""

import argparse
import getpass
import os
import subprocess


def main():
    """Main."""
    parser = argparse.ArgumentParser()
    parser.add_argument('paths_root', help='root directory to recursively scan for files.')
    parser.add_argument('--previous-all-paths', dest='previous_all_paths',
                        help='prior file with file paths, eg: /data/user/eevans/data-exp-2020-03-10T15:11:42.'
                        ' These files will be skipped.')
    parser.add_argument('--dryrun', default=False, action='store_true',
                        help='does everything except submitting the condor job(s)')
    parser.add_argument('--cpus', type=int, help='number of CPUs', default=8)
    parser.add_argument('--memory', help='amount of memory', default='20GB')
    args = parser.parse_args()

    for arg, val in vars(args).items():
        print(f'{arg}: {val}')

    # check paths in args
    for path in [args.paths_root, args.previous_all_paths]:
        if path and not os.path.exists(path):
            raise FileNotFoundError(path)

    # make condor scratch directory
    scratch = os.path.join('/scratch/', getpass.getuser(), 'all-paths')
    if not os.path.exists(scratch):
        os.makedirs(scratch)

    # make condor file
    condorpath = os.path.join(scratch, 'condor')
    with open(condorpath, 'w') as file:
        # args
        previous_arg = ''
        if args.previous_all_paths:
            previous_arg = f'--previous-all-paths {args.previous_all_paths}'
        staging_dir = os.path.join('/data/user/', getpass.getuser())
        transfer_input_files = ['all_paths.py', 'directory_scanner.py']

        # write
        file.write(f"""executable = {os.path.abspath('indexer_env.sh')}
arguments = python all_paths.py {args.paths_root} --staging-dir {staging_dir} --workers {args.cpus} {previous_arg}
output = {scratch}/all_paths.out
error = {scratch}/all_paths.err
log = {scratch}/all_paths.log
+FileSystemDomain = "blah"
should_transfer_files = YES
transfer_input_files = {",".join([os.path.abspath(f) for f in transfer_input_files])}
request_cpus = {args.cpus}
request_memory = {args.memory}
notification = Error
queue
""")

    # Execute
    if args.dryrun:
        print('Script Aborted: Condor job not submitted.')
    else:
        cmd = f'condor_submit {condorpath}'
        print(cmd)
        subprocess.check_call(cmd.split(), cwd=scratch)


if __name__ == '__main__':
    main()
