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


class IceCubeSeason:
    """Wrapper static class encapsulating season-name - season-year mapping logic."""
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

    @staticmethod
    def name_to_year(name):
        """Return the year of the season start for the season's name."""
        if name:
            for season_year, season_name in IceCubeSeason.SEASONS.items():
                if season_name == name:
                    return int(season_year)
            raise Exception(f"No season found for {name}.")
        else:
            return None

    @staticmethod
    def year_to_name(season_year):
        """Return the season's name for the year of the season start."""
        if season_year:
            try:
                return IceCubeSeason.SEASONS[str(season_year)]
            except KeyError:
                raise Exception(f"No season found for {season_year}.")
        else:
            return None


class ProcessingLevel:
    """Wrapper static class encapsulating processing-level parsing logic and levels constants."""
    PFRaw = "PFRaw"
    PFFilt = "PFFilt"
    PFDsT = "PFDsT"
    L2 = "L2"
    L3 = "L3"
    L4 = "L4"

    @staticmethod
    def from_path(dir_path):
        """Return the processing level parsed from the dir path, case insensitive."""
        # Ex: /data/exp/IceCube/2013/filtered/level2/1229/Run00123579_0/
        search_strings = {
            "PFRaw": ProcessingLevel.PFRaw,
            "PFFilt": ProcessingLevel.PFFilt,
            "PFDsT": ProcessingLevel.PFDsT,
            "Level2": ProcessingLevel.L2,
            "Level3": ProcessingLevel.L3,
            "Level4": ProcessingLevel.L4
        }
        dir_path_upper = dir_path.upper()
        for string, level in search_strings.items():
            if string.upper() in dir_path_upper:
                return level
        raise Exception(f"Cannot detect processing level, {dir_path}.")


class BasicFileMetadata:
    """Metadata for basic files"""
    file = None
    site = ""

    def __init__(self, file, site):
        self.file = file
        self.site = site

    def generate(self):
        """Gather the file's metadata."""
        metadata = dict()
        metadata['logical_name'] = self.file.path
        metadata['checksum'] = {'sha512': self.sha512sum()}
        metadata['file_size'] = self.file.stat().st_size
        metadata['locations'] = self._get_locations()
        metadata['create_date'] = date.fromtimestamp(os.path.getctime(self.file.path)).isoformat()
        return metadata

    def sha512sum(self):
        """Return the SHA512 checksum of the file given by path."""
        bufsize = 4194304
        sha = hashlib.new('sha512')
        with open(self.file.path, 'rb', buffering=0) as file:
            line = file.read(bufsize)
            while line:
                sha.update(line)
                line = file.read(bufsize)
        return sha.hexdigest()

    def _get_locations(self):
        """Return the locations object."""
        if '.tar' in self.file.name:
            return [{'site': self.site, 'path': self.file.path, 'archive': True}]
        return [{'site': self.site, 'path': self.file.path}]


class I3FileMetadata(BasicFileMetadata):
    """Metadata for i3 files"""
    processing_level = None
    season_year = None
    run = 0
    subrun = 0
    part = 0
    meta_xml = dict()

    def __init__(self, file, site, processing_level):
        super().__init__(file, site)
        self.processing_level = processing_level
        self._parse_filepath()

    def generate(self):
        """Gather the file's metadata."""
        metadata = super().generate()

        start_dt, end_dt, create_date, software = self._parse_xml()
        data_type = self._get_data_type()
        first_event, last_event, event_count, status = self._get_events_data()

        metadata['create_date'] = create_date  # Override BasicFileMetadata's value
        metadata['data_type'] = data_type
        metadata['processing_level'] = self.processing_level
        metadata['content_status'] = status
        metadata['software'] = software

        if data_type == "real":
            metadata['run'] = {
                'run_number': self.run,
                'subrun_number': self.subrun,
                'part_number': self.part,
                'start_datetime': start_dt,
                'end_datetime': end_dt,
                'first_event': first_event,
                'last_event': last_event,
                'event_count': event_count
            }
        return metadata

    def _parse_filepath(self):
        pass

    @staticmethod
    def parse_run_number(file):
        """Return run number from filename."""
        # Ex. Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst
        # Ex: Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst
        # Ex: Run00125791_GapsTxt.tar
        r = file.name.split('Run')[1]
        run = int(r.split('_')[0])
        return run

    def _get_data_type(self):
        """Return the file data type, real or simulation"""
        if "/exp/" in self.file.path:
            return "real"
        if "/sim/" in self.file.path:
            return "simulation"
        return None

    def _parse_xml(self):
        """Return data points from xml dict"""
        if self.meta_xml:
            start_dt = str(self.meta_xml['DIF_Plus']['Plus']['Start_DateTime'])
            end_dt = str(self.meta_xml['DIF_Plus']['Plus']['End_DateTime'])
            create_date = str(self.meta_xml['DIF_Plus']['DIF']['DIF_Creation_Date'])
            software = self._get_software()
        else:
            start_dt = None
            end_dt = None
            create_date = date.fromtimestamp(os.path.getctime(self.file.path)).isoformat()
            software = None

        return start_dt, end_dt, create_date, software

    def _get_software(self):
        """Return software metadata"""

        def parse_project(project):
            software = {}
            if 'Name' in project:
                software['name'] = str(project['Name'])
            if 'Version' in project:
                software['version'] = str(project['Version'])
            if 'DateTime' in project:
                software['date'] = str(project['DateTime'])
            return software

        software_list = []
        entry = self.meta_xml['DIF_Plus']['Plus']['Project']
        entry_type = type(entry)

        if entry_type is list:
            for project in entry:
                software_list.append(parse_project(project))
        elif entry_type is collections.OrderedDict:
            software_list = [parse_project(entry)]
        else:
            raise Exception(f"meta xml file has unanticipated 'Project' type {entry_type}.")

        return software_list

    def _get_events_data(self):
        """Return the first/last event ids, number of events, and content status"""
        first, last = None, None
        count = 0
        status = "good"
        try:
            for frame in dataio.I3File(self.file.path):
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


class L2FileMetadata(I3FileMetadata):
    """Metadata for L2 i3 files"""
    gaps_dict = dict()
    gcd_filepath = ""

    def __init__(self, file, site, dir_meta_xml, gaps_dict, gcd_filepath):
        super().__init__(file, site, ProcessingLevel.L2)
        self.meta_xml = dir_meta_xml
        self.gaps_dict = gaps_dict
        self.gcd_filepath = gcd_filepath

    def _parse_gaps_dict(self):
        """Return data points from gaps file, formatted"""
        if not self.gaps_dict:
            return None, None, None, None

        livetime = float(self.gaps_dict['File Livetime'])  # Ex: 0.92

        try:
            first = self.gaps_dict['First Event of File'].split()  # Ex: '53162019 2018 206130762188498'
            last = self.gaps_dict['Last Event of File'].split()  # Ex: '53164679 2018 206139955965204'

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

    def generate(self):
        """Gather the file's metadata."""
        metadata = super().generate()
        gaps, livetime, first_event_dict, last_event_dict = self._parse_gaps_dict()
        metadata['offline_processing_metadata'] = {
            # 'dataset_id': None,
            'season': self.season_year,
            'season_name': IceCubeSeason.year_to_name(self.season_year),
            'L2_gcd_file': self.gcd_filepath,
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

    def _parse_filepath(self):
        self.run = I3FileMetadata.parse_run_number(self.file)

        # Ex: Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst
        # Ex: Level2pass2_IC79.2010_data_Run00115975_Subrun00000000_00000055.i3.zst
        if re.match(r'(.*)(\.20\d{2})_data_Run[0-9]+_Subrun[0-9]+_[0-9]+(.*)', self.file.name):
            y = self.file.name.split('.')[1]
            self.season_year = int(y.split('_')[0])
            s = self.file.name.split('Subrun')[1]
            self.subrun = int(s.split('_')[0])
            p = self.file.name.split('_')[-1]
            self.part = int(p.split('.')[0])

        # Ex: Level2_IC86.2016_data_Run00129004_Subrun00000316.i3.bz2
        elif re.match(r'(.*)(\.20\d{2})_data_Run[0-9]+_Subrun[0-9]+(.*)', self.file.name):
            y = self.file.name.split('.')[1]
            self.season_year = int(y.split('_')[0])
            self.subrun = 0
            p = self.file.name.split('Subrun')[1]
            self.part = int(p.split('.')[0])

        # Ex: Level2_IC86.2011_data_Run00119221_Part00000126.i3.bz2
        elif re.match(r'(.*)(\.20\d{2})_data_Run[0-9]+_Part[0-9]+(.*)', self.file.name):
            y = self.file.name.split('.')[1]
            self.season_year = int(y.split('_')[0])
            self.subrun = 0
            p = self.file.name.split('Part')[1]
            self.part = int(p.split('.')[0])

        # Ex: Level2a_IC59_data_Run00115968_Part00000290.i3.gz
        elif re.match(r'(.*)(_IC.*)_data_Run[0-9]+_Part[0-9]+(.*)', self.file.name):
            n = self.file.name.split('IC')[1]
            strings = n.split('_')[0]
            self.season_year = IceCubeSeason.name_to_year(f"IC{strings}")
            self.subrun = 0
            p = self.file.name.split('Part')[1]
            self.part = int(p.split('.')[0])

        # Ex: Level2_All_Run00111562_Part00000046.i3.gz
        elif re.match(r'(.*)_Run[0-9]+(.*)_Part[0-9]+(.*)', self.file.name):
            self.season_year = None
            self.subrun = 0
            p = self.file.name.split('Part')[1]
            self.part = int(p.split('.')[0])

        else:
            raise Exception(f"Filename not in a known L2 file format, {self.file.name}.")

    @staticmethod
    def is_file(file):
        """True if the file is in the [...#].i3[...] file format."""
        # Ex: Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188.i3.zst
        # check if last char of filename (w/o extension) is an int
        return (".i3" in file.name) and (file.name.split('.i3')[0][-1]).isdigit()


class PFFiltFileMetadata(I3FileMetadata):
    """Metadata for PFFilt i3 files"""

    def __init__(self, file, site):
        super().__init__(file, site, ProcessingLevel.PFFilt)
        with tarfile.open(file.path) as tar:
            for tar_obj in tar:
                if ".meta.xml" in tar_obj.name:
                    self.meta_xml = xmltodict.parse(tar.extractfile(tar_obj))

    def generate(self):
        metadata = super().generate()
        return metadata

    def _parse_filepath(self):
        self.run = I3FileMetadata.parse_run_number(self.file)

        # Ex: PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2
        # Ex: PFFilt_PhysicsTrig_PhysicsFiltering_Run00121503_Subrun00000000_00000314.tar.bz2
        if re.match(r'PFFilt_(.*)_Run[0-9]+_Subrun[0-9]+_[0-9]+(.*)', self.file.name):
            self.season_year = None
            s = self.file.name.split('Subrun')[1]
            self.subrun = int(s.split('_')[0])
            p = self.file.name.split('_')[-1]
            self.part = int(p.split('.')[0])
        else:
            raise Exception(f"Filename not in a known PFFilt file format, {self.file.name}.")

    @staticmethod
    def is_file(file):
        """ True if the file is in the [...#].tar.bz2 file format. """
        # Ex. PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2
        # check if last char of filename (w/o extension) is an int
        return (".tar.bz2" in file.name) and (file.name.split('.tar.bz2')[0][-1]).isdigit()


class MetadataManager:
    """Commander class for handling metadata for different file types"""
    site = ""
    dir_processing_level = None
    l2_dir_metadata = dict()

    def __init__(self, path, site):
        self.site = site
        self.dir_processing_level = ProcessingLevel.from_path(path)

        if self.dir_processing_level == ProcessingLevel.L2:
            # get directory's metadata
            self._prep_l2_dir_metadata(path)

    def _prep_l2_dir_metadata(self, path):
        """Get metadata-related files for later processing with individual i3 files."""
        dir_meta_xml = None
        gaps_files = dict()  # gaps_files[<filename w/o extension>]
        gcd_files = dict()  # gcd_files[<run id w/o leading zeros>]
        for dir_entry in os.scandir(path):
            if not dir_entry.is_file():
                continue
            if "meta.xml" in dir_entry.name:  # Ex. level2_meta.xml, level2pass2_meta.xml
                if dir_meta_xml is not None:
                    raise Exception(f"Multiple *meta.xml files found in {path}.")
                with open(dir_entry.path, 'r') as xml:
                    dir_meta_xml = xmltodict.parse(xml.read())
            elif "_GapsTxt.tar" in dir_entry.name:  # Ex. Run00130484_GapsTxt.tar
                with tarfile.open(dir_entry.path) as tar:
                    for tar_obj in tar:
                        file_dict = yaml.safe_load(tar.extractfile(tar_obj))
                        # Ex. Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188_gaps.txt
                        no_extension = tar_obj.name.split("_gaps.txt")[0]
                        gaps_files[no_extension] = file_dict
            elif "GCD" in dir_entry.name:  # Ex. Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst
                run = I3FileMetadata.parse_run_number(dir_entry)
                gcd_files[str(run)] = dir_entry.path
        self.l2_dir_metadata['dir_meta_xml'] = dir_meta_xml
        self.l2_dir_metadata['gaps_files'] = gaps_files
        self.l2_dir_metadata['gcd_files'] = gcd_files

    def new_file(self, file):
        """Factory method for returning different metadata-file types"""
        # L2
        if self.dir_processing_level == ProcessingLevel.L2 and L2FileMetadata.is_file(file):
            try:
                no_extension = file.name.split(".i3")[0]
                gaps = self.l2_dir_metadata['gaps_files'][no_extension]
            except KeyError:
                gaps = dict()
            try:
                run = I3FileMetadata.parse_run_number(file)
                gcd = self.l2_dir_metadata['gcd_files'][str(run)]
            except KeyError:
                gcd = ""
            return L2FileMetadata(file, self.site, self.l2_dir_metadata['dir_meta_xml'], gaps, gcd)
        # PFRaw
        if self.dir_processing_level == ProcessingLevel.PFRaw:
            # TODO
            # ukey_b98a353f-72e8-4d2e-afd7-c41fa5c8d326_PFRaw_PhysicsFiltering_Run00131322_Subrun00000000_00000018.tar.gz
            # key_31445930_PFRaw_PhysicsFiltering_Run00128000_Subrun00000000_00000156.tar.gz
            # ukey_05815dd9-2411-468c-9bd5-e99b8f759efd_PFRaw_RandomFiltering_Run00130470_Subrun00000060_00000000.tar.gz
            # PFRaw_PhysicsTrig_PhysicsFiltering_Run00114085_Subrun00000000_00000208.tar.gz
            return None
        # PFFilt
        if self.dir_processing_level == ProcessingLevel.PFFilt and PFFiltFileMetadata.is_file(file):
            return PFFiltFileMetadata(file, self.site)
        # Other/ Basic
        return BasicFileMetadata(file, self.site)


def process_dir(path, site):
    """Return list of sub-directories and metadata of files in directory given by path."""
    try:
        scan = list(os.scandir(path))
    except (PermissionError, FileNotFoundError):
        scan = []
    dirs = []
    file_meta = []

    manager = MetadataManager(path, site)

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
                metadata_file = manager.new_file(dir_entry)
                metadata = metadata_file.generate()
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


async def request_post_patch(fc_rc, metadata):
    """POST metadata, and PATCH if file is already in the file catalog."""
    try:
        logging.debug('POSTing...')
        _ = await fc_rc.request("POST", '/api/files', metadata)
        logging.debug('POSTed.')
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            patch_path = e.response.json()['file']  # /api/files/{uuid}
            logging.debug('PATCHing...')
            _ = await fc_rc.request("PATCH", patch_path, metadata)
            logging.debug('PATCHed.')
        else:
            raise
    return fc_rc


async def main():
    """Main"""
    parser = argparse.ArgumentParser(description='Find files under PATH(s), compute their metadata and '
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
        fc_rc = await request_post_patch(fc_rc, metadata)

    fc_rc.close()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
