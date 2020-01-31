# indexer.py
"""Data-indexing script for File Catalog."""

import argparse
import asyncio
import collections
import hashlib
import logging
import os
import re
import tarfile
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from time import sleep

import requests
import xmltodict
import yaml
from icecube import dataclasses, dataio
from rest_tools.client import RestClient

logging.basicConfig(level=logging.DEBUG)

SEASONS = {
    '2005': 'ICstring9',
    '2006': 'IC9',
    '2007': 'IC22',
    '2008': 'IC40',
    '2009': 'IC59',
    '2010': 'IC79',
    '2011': 'IC86-1',
    '2012': 'IC86-2',
    '2013': 'IC86-3',
    '2014': 'IC86-4',
    '2015': 'IC86-5',
    '2016': 'IC86-6',
    '2017': 'IC86-7',
    '2018': 'IC86-8',
    '2019': 'IC86-9',
    '2020': 'IC86-10'
}


def _get_season_year(name):
    """Return the year of the season start for the season's name."""
    if name:
        for season_year, n in SEASONS.items():
            if n == name:
                return int(season_year)
        raise Exception(f"No season found for {name}.")
    else:
        return None


def _get_season_name(season_year):
    """Return the season's name for the year of the season start."""
    if season_year:
        try:
            return SEASONS[str(season_year)]
        except KeyError:
            raise Exception(f"No season found for {season_year}.")
    else:
        return None


def _get_processing_level(file):
    """Return the processing level parsed from the file's dir path, case insensitive."""
    levels = {
        "PFRaw": "PFRaw",
        "PFFilt": "PFFilt",
        "PFDsT": "PFDsT",
        "Level2": "L2",
        "Level3": "L3",
        "Level4": "L4"
    }
    dir_path_upper = os.path.dirname(file).upper()
    for level, level_for_metadata in levels.items():
        if level.upper() in dir_path_upper:
            return level_for_metadata
    raise Exception(f"Cannot detect file's processing level, {file.path}.")


def _get_run_number(file):
    """Return run number from filename."""
    # Ex. Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst
    # Ex: Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst
    # Ex: Run00125791_GapsTxt.tar
    r = file.name.split('Run')[1]
    run = int(r.split('_')[0])
    return run


def _disect_filepath(file):
    """Return data_type, processing_level, year, run, subrun, & part number from file's path."""
    # Ex: /data/exp/IceCube/2013/filtered/level2/1229/Run00123579_0/Level2_IC86.2013_data_Run00123579_Subrun00000004.i3.bz2
    processing_level = _get_processing_level(file)

    def _get_year_from_year(file):
        # Ex: Level2_IC86.2017_[...]
        y = file.name.split('.')[1]
        year = int(y.split('_')[0])
        return year

    run = _get_run_number(file)
    subrun = 0

    # L2
    if processing_level == "L2":
        # Ex: Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst
        # Ex: Level2pass2_IC79.2010_data_Run00115975_Subrun00000000_00000055.i3.zst
        if re.match(r'(.*)(\.20\d{2})_data_Run[0-9]+_Subrun[0-9]+_[0-9]+(.*)', file.name):
            year = _get_year_from_year(file)
            s = file.name.split('Subrun')[1]
            subrun = int(s.split('_')[0])
            p = file.name.split('_')[-1]
            part = int(p.split('.')[0])

        # Ex: Level2_IC86.2016_data_Run00129004_Subrun00000316.i3.bz2
        elif re.match(r'(.*)(\.20\d{2})_data_Run[0-9]+_Subrun[0-9]+(.*)', file.name):
            year = _get_year_from_year(file)
            p = file.name.split('Subrun')[1]
            part = int(p.split('.')[0])

        # Ex: Level2_IC86.2011_data_Run00119221_Part00000126.i3.bz2
        elif re.match(r'(.*)(\.20\d{2})_data_Run[0-9]+_Part[0-9]+(.*)', file.name):
            year = _get_year_from_year(file)
            p = file.name.split('Part')[1]
            part = int(p.split('.')[0])

        # Ex: Level2a_IC59_data_Run00115968_Part00000290.i3.gz
        elif re.match(r'(.*)(_IC.*)_data_Run[0-9]+_Part[0-9]+(.*)', file.name):
            n = file.name.split('IC')[1]
            strings = n.split('_')[0]
            year = _get_season_year(f"IC{strings}")
            p = file.name.split('Part')[1]
            part = int(p.split('.')[0])

        # Ex: Level2_All_Run00111562_Part00000046.i3.gz
        elif re.match(r'(.*)_Run[0-9]+(.*)_Part[0-9]+(.*)', file.name):
            year = None
            p = file.name.split('Part')[1]
            part = int(p.split('.')[0])

        else:
            raise Exception(f"Filename not in a known L2 file format, {file.name}.")

    # PFFilt
    elif processing_level == "PFFilt":
        # Ex: PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2
        # Ex: PFFilt_PhysicsTrig_PhysicsFiltering_Run00121503_Subrun00000000_00000314.tar.bz2
        if re.match(r'PFFilt_(.*)_Run[0-9]+_Subrun[0-9]+_[0-9]+(.*)', file.name):
            year = None
            s = file.name.split('Subrun')[1]
            subrun = int(s.split('_')[0])
            p = file.name.split('_')[-1]
            part = int(p.split('.')[0])

        else:
            raise Exception(f"Filename not in a known PFFilt file format, {file.name}.")

    # Other/Unknown
    else:
        raise Exception(f"There are no known file formats for {processing_level} file, {file.name}.")

    return processing_level, year, run, subrun, part


def _get_software(meta_xml):
    """Return software metadata"""

    def _parse_project(project):
        software = {}
        if 'Name' in project:
            software['name'] = str(project['Name'])
        if 'Version' in project:
            software['version'] = str(project['Version'])
        if 'DateTime' in project:
            software['date'] = str(project['DateTime'])
        return software

    software_list = []
    entry = meta_xml['DIF_Plus']['Plus']['Project']
    entry_type = type(entry)

    if entry_type is list:
        for project in entry:
            software_list.append(_parse_project(project))
    elif entry_type is collections.OrderedDict:
        software_list = [_parse_project(entry)]
    else:
        raise Exception(f"meta xml file has unanticipated 'Project' type {entry_type}.")

    return software_list


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
        return "simulation"
    return None


def _parse_xml(file, meta_xml):
    """Return data points from xml dict"""
    if meta_xml:
        start_dt = str(meta_xml['DIF_Plus']['Plus']['Start_DateTime'])
        end_dt = str(meta_xml['DIF_Plus']['Plus']['End_DateTime'])
        create_date = str(meta_xml['DIF_Plus']['DIF']['DIF_Creation_Date'])
        software = _get_software(meta_xml)
    else:
        start_dt = None
        end_dt = None
        create_date = date.fromtimestamp(os.path.getctime(file.path)).isoformat()
        software = None

    return start_dt, end_dt, create_date, software


def _parse_gaps_dict(gaps_dict):
    """Return data points from gaps file, formatted"""
    if not gaps_dict:
        return None, None, None, None

    livetime = float(gaps_dict['File Livetime'])  # Ex: 0.92

    try:
        first = gaps_dict['First Event of File'].split()  # Ex: '53162019 2018 206130762188498'
        last = gaps_dict['Last Event of File'].split()  # Ex: '53164679 2018 206139955965204'

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
    except KeyError:
        return None, livetime, None, None


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


def _get_basic_metadata(file, site):
    """Just return the basic metadata."""
    metadata = {
        'logical_name': file.path,
        'checksum': {'sha512': sha512sum(file.path)},
        'file_size': file.stat().st_size,
        'locations': [{'site': site, 'path': file.path}],
        'create_date': date.fromtimestamp(os.path.getctime(file.path)).isoformat()
    }
    return metadata


def _get_i3_part_file_metadata(file, site, meta_xml=None, gaps_dict=None, gcd_filepath=""):
    """Return metadata for one file"""
    start_dt, end_dt, create_date, software = _parse_xml(file, meta_xml)
    processing_level, season_year, run, subrun, part = _disect_filepath(file)
    data_type = _get_data_type(file)
    first_event, last_event, event_count, status = _get_events_data(file)
    basic_metadata = _get_basic_metadata(file, site)

    metadata = {
        'logical_name': file.path,
        'checksum': basic_metadata['checksum'],
        'file_size': basic_metadata['file_size'],
        'locations': basic_metadata['locations'],
        'create_date': create_date,
        'data_type': data_type,
        'processing_level': processing_level,
        'content_status': status,
        'software': software,
    }
    if data_type == "real":
        metadata['run'] = {
            'run_number': run,
            'subrun_number': subrun,
            'part_number': part,
            'start_datetime': start_dt,
            'end_datetime': end_dt,
            'first_event': first_event,
            'last_event': last_event,
            'event_count': event_count
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
    # if data_type == "simulation":
    #     metadata['simulation'] = {
    #         ...
    #     }
    if processing_level in ("L2", "L3"):
        gaps, livetime, first_event_dict, last_event_dict = _parse_gaps_dict(gaps_dict)
        metadata['offline_processing_metadata'] = {
            # 'dataset_id': None,
            'season': season_year,
            'season_name': _get_season_name(season_year),
            'L2_gcd_file': gcd_filepath,
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


def _is_i3_part_file(file):
    """Return True if the file is in the [...#].i3[...] file format."""
    # Ex: Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188.i3.zst
    # check if last char of filename (w/o extension) is an int
    return (".i3" in file.name) and (file.name.split('.i3')[0][-1]).isdigit()


def _is_i3_part_tar_bz2(file):
    """Return True if the file is in the [...#].tar.bz2 file format."""
    # Ex. PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2
    # check if last char of filename (w/o extension) is an int
    return (".tar.bz2" in file.name) and (file.name.split('.tar.bz2')[0][-1]).isdigit()


def _get_directory_metadata(scan, path):
    """Get metadata-related files for later processing with individual i3 files."""
    dir_meta_xml = None
    gaps_files = dict()  # gaps_files[<filename w/o extension>]
    gcd_files = dict()  # gcd_files[<run id w/o leading zeros>]

    for dir_entry in scan:
        if "meta.xml" in dir_entry.name:  # Ex. level2_meta.xml, level2pass2_meta.xml
            if dir_meta_xml is not None:
                raise Exception(f"Multiple *meta.xml files found in {path}.")
            with open(dir_entry.path, 'r') as f:
                dir_meta_xml = xmltodict.parse(f.read())
        if "_GapsTxt.tar" in dir_entry.name:  # Ex. Run00130484_GapsTxt.tar
            with tarfile.open(dir_entry.path) as tar:
                for tar_obj in tar:
                    file_dict = yaml.safe_load(tar.extractfile(tar_obj))
                    # Ex. Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188_gaps.txt
                    gaps_files[tar_obj.name.split("_gaps.txt")[0]] = file_dict
        if "GCD" in dir_entry.name:  # Ex. Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst
            gcd_files[str(_get_run_number(dir_entry))] = dir_entry.path

    return dir_meta_xml, gaps_files, gcd_files


def process_dir(path, site):
    """Return list of sub-directories and metadata of files in directory given by path."""
    try:
        scan = list(os.scandir(path))
    except (PermissionError, FileNotFoundError):
        scan = []
    dirs = []
    file_meta = []

    # get directory's metadata
    dir_meta_xml, gaps_files, gcd_files = _get_directory_metadata(scan, path)

    # get files' metadata
    for dir_entry in scan:
        if dir_entry.is_symlink():
            continue
        elif dir_entry.is_dir():
            logging.debug(f'Directory appended, {dir_entry.path}')
            dirs.append(dir_entry.path)
        elif dir_entry.is_file():
            logging.debug(f'Gathering metadata for {dir_entry.name}...')
            try:
                if _is_i3_part_file(dir_entry):
                    # Ex. Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188.i3*
                    try:
                        gaps = gaps_files[dir_entry.name.split(".i3")[0]]
                    except KeyError:
                        gaps = dict()
                    try:
                        gcd = gcd_files[str(_get_run_number(dir_entry))]
                    except KeyError:
                        gcd = ""
                    metadata = _get_i3_part_file_metadata(dir_entry, site,
                                                          meta_xml=dir_meta_xml,
                                                          gaps_dict=gaps,
                                                          gcd_filepath=gcd)
                elif _is_i3_part_tar_bz2(dir_entry):
                    # Ex. PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2
                    # read in the meta.xml file from the tar
                    with tarfile.open(dir_entry.path) as tar:
                        for tar_obj in tar:
                            if ".meta.xml" in tar_obj.name:
                                file_meta_xml = xmltodict.parse(tar.extractfile(tar_obj))
                                break
                    metadata = _get_i3_part_file_metadata(dir_entry, site,
                                                          meta_xml=file_meta_xml)
                else:
                    metadata = _get_basic_metadata(dir_entry, site)
            # OSError is thrown for special files like sockets
            except (OSError, PermissionError, FileNotFoundError) as e:
                logging.exception(f'{dir_entry.name} not gathered, {e.__class__.__name__}.')
                continue
            file_meta.append(metadata)
            logging.debug(f'{dir_entry.name} gathered.')

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


async def _request_autoreconnect(fc_rc, method, path, metadata, url):
    """Request and automatically reconnect if needed."""
    while True:
        try:
            logging.debug(f'{method}ing...')
            _ = await fc_rc.request(method, path, metadata)
            logging.debug(f'{method}ed.')
            return fc_rc
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 504:
                logging.warning('Server Error, Gateway Time-out')
            else:
                raise
        except requests.exceptions.ConnectionError:
            logging.warning('Connection Error')

        fc_rc.close()
        sleep(10)
        logging.warning('Reconnecting and trying again...')
        fc_rc = RestClient(url, token=fc_rc.token, timeout=fc_rc.timeout, retries=fc_rc.retries)


async def request_post_patch(fc_rc, metadata, url):
    """POST metadata, and PATCH if file is already in the file catalog."""
    try:
        fc_rc = await _request_autoreconnect(fc_rc, "POST", '/api/files', metadata, url)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            patch_path = e.response.json()['file']  # /api/files/{uuid}
            fc_rc = await _request_autoreconnect(fc_rc, "PATCH", patch_path, metadata, url)
        else:
            raise
    return fc_rc


async def main():
    """Main"""
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
    parser.add_argument('--timeout', type=int, default=15, help='REST client timeout duration')
    parser.add_argument('--retries', type=int, default=3, help='REST client number of retries')
    args = parser.parse_args()

    for arg, val in vars(args).items():
        logging.info(f'{arg}: {val}')

    fc_rc = RestClient(args.url, token=args.token, timeout=args.timeout, retries=args.retries)

    logging.info(f'Collecting metadata from {args.path}...')

    # POST each file's metadata to file catalog
    for metadata in gather_file_info(args.path, args.site):
        logging.info(metadata)
        fc_rc = await request_post_patch(fc_rc, metadata, args.url)

    fc_rc.close()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
