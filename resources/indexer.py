# indexer.py
"""Data-indexing script for File Catalog."""

import argparse
import asyncio
import hashlib
import os
import tarfile
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from time import sleep

import requests
import xmltodict
import yaml
from icecube import dataclasses, dataio
from rest_tools.client import RestClient


def _get_processing_level(file):
    """Return the processing level parsed from the filename"""
    levels = {
        "PFRaw": "PFRaw",
        "PFilt": "PFilt",
        "PFDsT": "PFDsT",
        "Level2": "L2",
        "Level3": "L3",
        "Level4": "L4"
    }
    for level, level_for_metadata in levels.items():
        if level in file.name:
            return level_for_metadata
    return ""


def _get_run_number(file):
    """Return run number from filename."""
    # Ex. Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst
    # Ex: Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst
    # Ex: Run00125791_GapsTxt.tar
    pre_run = file.name.split('Run')[1]
    run = int(pre_run.split('_')[0])
    return run


def _get_subrun_number(file):
    """Return subrun number from filename"""
    # Ex: Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst
    pre_subrun = file.name.split('Subrun')[1]
    subrun = int(pre_subrun.split('_')[0])
    return subrun


def _disect_filename(file):
    """Return year/run/subrun/part number from filename"""
    # Ex: Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst
    pre_year = file.name.split('.')[1]
    year = int(pre_year.split('_')[0])

    run = _get_run_number(file)
    subrun = _get_subrun_number(file)

    pre_part = file.name.split('_')[-1]
    part = int(pre_part.split('.')[0])

    return year, run, subrun, part


def _get_software(run_meta_xml):
    """Return software metadata"""
    softwares = []
    for software in run_meta_xml['DIF_Plus']['Plus']['Project']:
        soft_meta = {
            'name': str(software['Name']),
            'version': str(software['Version']),
            'date': str(software['DateTime'])
        }
        softwares.append(soft_meta)
    return softwares


def _get_events_data(file):
    """Return the first/last event ids, number of events, and content status"""
    first, last = None, None
    count = 0
    status = "good"
    try:
        for frame in dataio.I3File(file.path):
            if 'I3EventHeader' in frame:
                count = count + 1
                event_id = int(frame['I3EventHeader'].event_id)
                if first is None or first > event_id:
                    first = event_id
                if last is None or last < event_id:
                    last = event_id
    except:
        status = "bad"
    return first, last, count, status


def _get_data_type(file):
    """Return the file data type, real or simulation"""
    if "/exp/" in file.path:
        return "real"
    if "/sim/" in file.path:
        return "simulaton"
    return None


def _get_season_name(year):
    if 2004 < year < 2011:
        seasons = {
            '2005': 'ICstring9',
            '2006': 'IC9',
            '2007': 'IC22',
            '2008': 'IC40',
            '2009': 'IC59',
            '2010': 'IC79'
        }
        return seasons[str(year)]
    if year < 2021:
        return f'I86-{year - 2010}'
    raise Exception(f"No season name found for year, {year}")


def _parse_xml(path, run_meta_xml):
    """Return data points from xml dict"""
    if run_meta_xml:
        start_dt = str(run_meta_xml['DIF_Plus']['Plus']['Start_DateTime'])
        end_dt = str(run_meta_xml['DIF_Plus']['Plus']['End_DateTime'])
        create_date = str(run_meta_xml['DIF_Plus']['DIF']['DIF_Creation_Date'])
        software = _get_software(run_meta_xml)
    else:
        start_dt = None
        end_dt = None
        create_date = date.fromtimestamp(os.path.getctime(path)).isoformat()
        software = None

    return start_dt, end_dt, create_date, software


def _parse_gaps_dict(gaps_dict):
    """Return data points from gaps file, formatted"""
    if not gaps_dict:
        return None, None, None, None

    first = gaps_dict['First Event of File'].split()  # Ex: '53162019 2018 206130762188498'
    last = gaps_dict['Last Event of File'].split()  # Ex: '53164679 2018 206139955965204'
    livetime = float(gaps_dict['File Livetime'])  # Ex: 0.92

    first_id = int(first[0])
    first_dt = dataclasses.I3Time(int(first[1]), int(first[2])).date_time
    last_id = int(last[0])
    last_dt = dataclasses.I3Time(int(last[1]), int(last[2])).date_time

    gaps = [{
        'start_event_id': first_id,
        'stop_event_id': last_id,
        'delta_time': (last_dt - first_dt).total_seconds(),
        'start_date': first_dt.isoformat(),
        'stop_date': last_dt.isoformat()
    }]

    first_event_dict = {'event_id': first_id, 'datetime': first_dt.isoformat()}
    last_event_dict = {'event_id': last_id, 'datetime': last_dt.isoformat()}

    return gaps, livetime, first_event_dict, last_event_dict


def _get_file_metadata(file, site, run_meta_xml=None, gaps_dict=None, gcd_files=""):
    """Return metadata for one file"""
    path = file.path
    start_dt, end_dt, create_date, software = _parse_xml(path, run_meta_xml)
    first_event, last_event, count, status = _get_events_data(file)
    data_type = _get_data_type(file)
    processing_level = _get_processing_level(file)
    year, run, subrun, part = _disect_filename(file)

    metadata = {
        'logical_name': path,
        'checksum': {'sha512': sha512sum(path)},
        'file_size': file.stat().st_size,
        'locations': [{'site': site, 'path': path}],
        'create_date': create_date,
        'data_type': data_type,
        'processing_level': processing_level,
        'content_status': status,
        'software': software,
        'start_datetime': start_dt,
        'end_datetime': end_dt,
        'run_number': run,
        'subrun_number': subrun,
        'first_event': first_event,
        'last_event': last_event,
        'events': count
    }
    # if True:
    #     metadata['iceprod'] = {
    #         'dataset': None,
    #         'dataset_id': None,
    #         'job': None,
    #         'job_id': None,
    #         'task': None,
    #         'task_id': None,
    #         'config': None
    #     }
    # if data_type == "simulaton":
    #     metadata['simulaton'] = {
    #         'generator': None,
    #         'energy_min': None,
    #         'energy_max': None
    #     }
    if processing_level in ("L2", "L3"):
        gaps, livetime, first_event_dict, last_event_dict = _parse_gaps_dict(gaps_dict)
        metadata['offline_processing_metadata'] = {
            # 'dataset_id': None,
            'season': year,
            'season_name': _get_season_name(year),
            'part': part,
            'L2_gcd_file': gcd_files,
            # 'L2_snapshot_id': None,
            # 'L2_production_version': None,
            # 'L3_source_dataset_id': None,
            # 'working_group': None,
            # 'validation_validated': None,
            # 'validation_date': None,
            # 'validation_software': {},
            'livetime': livetime,
            'gaps': gaps,
            'first_event': first_event_dict,
            'last_event': last_event_dict
        }

    return metadata


def sha512sum(path):
    """Return the SHA512 checksum of the file given by path."""
    bufsize = 4194304
    h = hashlib.new('sha512')
    with open(path, 'rb', buffering=0) as f:
        line = f.read(bufsize)
        while line:
            h.update(line)
            line = f.read(bufsize)
    return h.hexdigest()


def _look_at_file(file):
    """Return True if the file is in the .i3.zst file format"""
    if "GCD.i3.zst" in file.name:
        return False
    if "_IT.i3.zst" in file.name:
        return False
    return ".i3.zst" in file.name


def process_dir(path, site):
    """Return list of sub-directories and metadata of files in directory given by path."""
    try:
        scan = list(os.scandir(path))
    except (PermissionError, FileNotFoundError):
        scan = []
    dirs = []
    file_meta = []

    # get directory's metadata
    run_meta_xml = None
    gaps_files = dict()  # gaps_files[<filename w/o extension>]
    gcd_files = dict()  # gcd_files[<run id w/o leading zeros>]
    for dir_entry in scan:
        if "meta.xml" in dir_entry.name:  # Ex. level2_meta.xml
            with open(dir_entry.path, 'r') as f:
                run_meta_xml = xmltodict.parse(f.read())
        if "_GapsTxt.tar" in dir_entry.name:  # Ex. Run00130484_GapsTxt.tar
            with tarfile.open(dir_entry.path) as tar:
                for tar_obj in tar:
                    file = tar.extractfile(tar_obj)
                    file_dict = yaml.load(file)
                    # Ex. Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188_gaps.txt
                    name = tar_obj.name.split("_gaps.txt")[0]
                    gaps_files[name] = file_dict
        if "GCD" in dir_entry.name:  # Ex. Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst
            run = _get_run_number(dir_entry)
            gcd_files[str(run)] = dir_entry.path

    # get files' metadata
    for dir_entry in scan:
        if dir_entry.is_symlink():
            continue
        elif dir_entry.is_dir():
            dirs.append(dir_entry.path)
        elif dir_entry.is_file():
            if not _look_at_file(dir_entry):
                continue
            try:
                # Ex. Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188.i3.zst
                filename_no_extension = dir_entry.name.split(".i3.zst")[0]
                gaps = gaps_files[filename_no_extension]
                run = _get_run_number(dir_entry)
                gcd = gcd_files[str(run)]
                metadata = _get_file_metadata(dir_entry, site, run_meta_xml, gaps, gcd)
            # OSError is thrown for special files like sockets
            except (OSError, PermissionError, FileNotFoundError):
                continue
            file_meta.append(metadata)

    return dirs, file_meta


def gather_file_info(dirs, site):
    """Return an iterator for metadata of files recursively found under dirs."""
    dirs = [os.path.abspath(p) for p in dirs]
    futures = []
    with ProcessPoolExecutor() as pool:
        while futures or dirs:
            for d in dirs:
                futures.append(pool.submit(process_dir, d, site))
            while not futures[0].done():  # concurrent.futures.wait(FIRST_COMPLETED) is slower
                sleep(0.1)
            future = futures.pop(0)
            dirs, file_meta = future.result()
            yield from file_meta


async def main():
    parser = argparse.ArgumentParser(
            description='Find files under PATH(s), compute their metadata and '
                        'upload it to File Catalog.',
            epilog='Notes: (1) symbolic links are never followed.',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('path', metavar='PATH', nargs='+',
                        help='path(s) to scan for files.')
    parser.add_argument('-u', '--url', default='https://file-catalog.icecube.wisc.edu/',  # 'http://localhost:8888'
                        help='File Catalog URL')
    parser.add_argument('-s', '--site', required=True,
                        help='site value of the "locations" object')
    parser.add_argument('-t', '--token', required=True,
                        help='LDAP token')
    args = parser.parse_args()

    fc_rc = RestClient(args.url, token=args.token, timeout=15, retries=3)

    # POST each file's metadata to file catalog
    for metadata in gather_file_info(args.path, args.site):
        print(metadata)
        """
        try:
            _ = await fc_rc.request('POST', '/api/files', metadata)
        except requests.exceptions.HTTPError as e:
            # PATCH if file is already in the file catalog
            if e.response.status_code == 409:
                file_path = e.response.json()['file']  # /api/files/{uuid}
                _ = await fc_rc.request('PATCH', file_path, metadata)
            else:
                raise
        """


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
