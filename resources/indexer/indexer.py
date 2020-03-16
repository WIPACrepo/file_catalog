# indexer.py
"""Data-indexing script for File Catalog."""

import argparse
import asyncio
import collections
import hashlib
import logging
import math
import os
import pathlib
import re
import tarfile
import xml
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from time import sleep

import requests
import xmltodict
import yaml
from icecube import dataclasses, dataio
from rest_tools.client import RestClient

ACCEPTED_ROOTS = ['/data/']
TAR_EXTENSIONS = ('.tar.gz', '.tar.bz2', '.tar.zst')


def is_processable_path(path):
    """Return True if path is not a symlink, a socket, a fifo, a device, nor char device."""
    plp = pathlib.Path(path)
    return not (plp.is_symlink() or plp.is_socket() or plp.is_fifo() or plp.is_block_device() or plp.is_char_device())


class FileInfo:
    """Wrapper around common file information. Similar to os.DirEntry."""

    def __init__(self, filepath):
        self.path = filepath
        self.name = os.path.basename(self.path)
        self.stat = lambda: os.stat(self.path)


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
        if not name:
            return None
        for season_year, season_name in IceCubeSeason.SEASONS.items():
            if season_name == name:
                return int(season_year)
        raise Exception(f"No season found for {name}.")

    @staticmethod
    def year_to_name(season_year):
        """Return the season's name for the year of the season start."""
        if not season_year:
            return None
        try:
            return IceCubeSeason.SEASONS[str(season_year)]
        except KeyError:
            raise Exception(f"No season found for {season_year}.")


class ProcessingLevel:
    """Wrapper static class encapsulating processing-level parsing logic and levels constants."""
    PFRaw = "PFRaw"
    PFFilt = "PFFilt"
    PFDST = "PFDST"
    L2 = "L2"
    L3 = "L3"
    L4 = "L4"

    @staticmethod
    def from_filename(path):
        """Return the processing level parsed from the file name, case insensitive."""
        # Ex: Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst
        search_strings = {
            "PFRAW": ProcessingLevel.PFRaw,
            "PFFILT": ProcessingLevel.PFFilt,
            "PFDST": ProcessingLevel.PFDST,
            "LEVEL2": ProcessingLevel.L2,
            "LEVEL3": ProcessingLevel.L3,
            "LEVEL4": ProcessingLevel.L4
        }
        path_upper = path.upper()
        for string, level in search_strings.items():
            if string in path_upper:
                return level
        return None


class BasicFileMetadata:
    """Metadata for basic files"""

    def __init__(self, file, site):
        self.file = file
        self.site = site

    def generate(self):
        """Gather the file's metadata."""
        metadata = {}
        metadata['logical_name'] = self.file.path
        metadata['checksum'] = {'sha512': self.sha512sum()}
        metadata['file_size'] = self.file.stat().st_size
        metadata['locations'] = [{'site': self.site, 'path': self.file.path}]
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


class I3FileMetadata(BasicFileMetadata):
    """Metadata for i3 files"""

    def __init__(self, file, site, processing_level):
        super().__init__(file, site)
        self.processing_level = processing_level
        self.season_year = None
        self.run = 0
        self.subrun = 0
        self.part = 0
        self.meta_xml = {}
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
        """Set the year, run, subrun, and part from the file name."""
        raise NotImplementedError()

    @staticmethod
    def parse_run_number(file):
        """Return run number from filename."""
        # Ex. Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst
        # Ex: Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst
        # Ex: Run00125791_GapsTxt.tar
        # Ex: Level2_IC86.2015_24HrTestRuns_data_Run00126291_Subrun00000203.i3.bz2
        filename = file.name
        if '24HrTestRuns' in filename:  # hard-coded fix
            filename = filename.split('24HrTestRuns')[1]

        r = filename.split('Run')[1]
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
        start_dt = None
        end_dt = None
        create_date = None
        software = None

        if self.meta_xml:
            try:
                start_dt = str(self.meta_xml['DIF_Plus']['Plus']['Start_DateTime'])
            except KeyError:
                pass
            try:
                end_dt = str(self.meta_xml['DIF_Plus']['Plus']['End_DateTime'])
            except KeyError:
                pass
            try:
                create_date = str(self.meta_xml['DIF_Plus']['DIF']['DIF_Creation_Date'])
            except KeyError:
                pass
            try:
                software = self._get_software()
            except KeyError:
                pass

        if not create_date:
            create_date = date.fromtimestamp(os.path.getctime(self.file.path)).isoformat()

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

    def _grab_meta_xml_from_tar(self):
        """Open tar file and set *meta.xml as self.meta_xml."""
        try:
            with tarfile.open(self.file.path) as tar:
                for tar_obj in tar:
                    if ".meta.xml" in tar_obj.name:
                        self.meta_xml = xmltodict.parse(tar.extractfile(tar_obj))
        except (xml.parsers.expat.ExpatError, tarfile.ReadError, EOFError):
            pass

    @staticmethod
    def _is_run_tar_file(file):
        """ True if the file is in the [...#].tar.[...] file format. """
        # Ex. PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2
        # check if last char of filename (w/o extension) is an int
        for ext in TAR_EXTENSIONS:
            if (ext in file.name) and (file.name.split(ext)[0][-1]).isdigit():
                return True
        return False


class L2FileMetadata(I3FileMetadata):
    """Metadata for L2 i3 files"""

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
        if livetime < 0:  # corrupted value, don't read any more values
            return None, None, None, None

        try:
            # Ex: '53162019 2018 206130762188498'
            first = self.gaps_dict['First Event of File'].split()
            # Ex: '53164679 2018 206139955965204'
            last = self.gaps_dict['Last Event of File'].split()

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
        """Set the year, run, subrun, and part from the file name."""
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
        # Ex: Level2_IC86.2012_Test_data_Run00120028_Subrun00000081.i3.bz2
        # Ex: Level2_IC86.2015_24HrTestRuns_data_Run00126291_Subrun00000203.i3.bz2
        elif re.match(r'(.*)(\.20\d{2})(.*)_data_Run[0-9]+_Subrun[0-9]+(.*)', self.file.name):
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
    def is_file(file, processing_level):
        """True if the file is in the [...#].i3[...] file format."""
        # Ex: Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188.i3.zst
        # check if last char of filename (w/o extension) is an int
        return (processing_level == ProcessingLevel.L2) and (".i3" in file.name) and (file.name.split('.i3')[0][-1]).isdigit()


class PFFiltFileMetadata(I3FileMetadata):
    """Metadata for PFFilt i3 files"""

    def __init__(self, file, site):
        super().__init__(file, site, ProcessingLevel.PFFilt)
        self._grab_meta_xml_from_tar()

    def _parse_filepath(self):
        """Set the year, run, subrun, and part from the file name."""
        self.run = I3FileMetadata.parse_run_number(self.file)

        # Ex: PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2
        # Ex: PFFilt_PhysicsTrig_PhysicsFiltering_Run00121503_Subrun00000000_00000314.tar.bz2
        # Ex: orig.PFFilt_PhysicsFiltering_Run00127080_Subrun00000000_00000244.tar.bz2.orig
        if re.match(r'(.*)PFFilt_(.*)_Run[0-9]+_Subrun[0-9]+_[0-9]+(.*)', self.file.name):
            self.season_year = None
            s = self.file.name.split('Subrun')[1]
            self.subrun = int(s.split('_')[0])
            p = self.file.name.split('_')[-1]
            self.part = int(p.split('.')[0])
        # Ex: PFFilt_PhysicsTrig_PhysicsFilt_Run00089959_00180.tar.gz
        # Ex: PFFilt_PhysicsTrig_RandomFilt_Run86885_006.tar.gz
        elif re.match(r'PFFilt_(.*)_Run[0-9]+_[0-9]+(.*)', self.file.name):
            self.season_year = None
            self.subrun = 0
            p = self.file.name.split('_')[-1]
            self.part = int(p.split('.')[0])
        else:
            raise Exception(f"Filename not in a known PFFilt file format, {self.file.name}.")

    @staticmethod
    def is_file(file, processing_level):
        """ True if PFFilt and the file is in the [...#].tar.[...] file format. """
        # Ex. PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2
        return (processing_level == ProcessingLevel.PFFilt) and I3FileMetadata._is_run_tar_file(file)


class PFDSTFileMetadata(I3FileMetadata):
    """Metadata for PFDST i3 files"""

    def __init__(self, file, site):
        super().__init__(file, site, ProcessingLevel.PFDST)
        self._grab_meta_xml_from_tar()

    def _parse_filepath(self):
        """Set the year, run, subrun, and part from the file name."""
        self.run = I3FileMetadata.parse_run_number(self.file)

        # Ex. ukey_fa818e64-f6d2-4cc1-9b34-e50bfd036bf3_PFDST_PhysicsFiltering_Run00131437_Subrun00000000_00000066.tar.gz
        # Ex: ukey_42c89a63-e3f7-4c3e-94ae-840eff8bd4fd_PFDST_RandomFiltering_Run00131155_Subrun00000051_00000000.tar.gz
        # Ex: PFDST_PhysicsFiltering_Run00125790_Subrun00000000_00000064.tar.gz
        # Ex: PFDST_UW_PhysicsFiltering_Run00125832_Subrun00000000_00000000.tar.gz
        # Ex: PFDST_RandomFiltering_Run00123917_Subrun00000000_00000000.tar.gz
        # Ex: PFDST_PhysicsTrig_PhysicsFiltering_Run00121663_Subrun00000000_00000091.tar.gz
        # Ex: PFDST_TestData_PhysicsFiltering_Run00122158_Subrun00000000_00000014.tar.gz
        # Ex: PFDST_TestData_RandomFiltering_Run00119375_Subrun00000136_00000000.tar.gz
        # Ex: PFDST_TestData_Unfiltered_Run00119982_Subrun00000000_000009.tar.gz
        if re.match(r'(.*)_Run[0-9]+_Subrun[0-9]+_[0-9]+(.*)', self.file.name):
            self.season_year = None
            s = self.file.name.split('Subrun')[1]
            self.subrun = int(s.split('_')[0])
            p = self.file.name.split('_')[-1]
            self.part = int(p.split('.')[0])
        else:
            raise Exception(f"Filename not in a known PFDST file format, {self.file.name}.")

    @staticmethod
    def is_file(file, processing_level):
        """ True if PFDST and the file is in the [...#].tar.[...] file format. """
        # Ex.
        # ukey_fa818e64-f6d2-4cc1-9b34-e50bfd036bf3_PFDST_PhysicsFiltering_Run00131437_Subrun00000000_00000066.tar.gz
        return (processing_level == ProcessingLevel.PFDST) and I3FileMetadata._is_run_tar_file(file)


class PFRawFileMetadata(I3FileMetadata):
    """Metadata for PFRaw i3 files"""

    def __init__(self, file, site):
        super().__init__(file, site, ProcessingLevel.PFRaw)
        self._grab_meta_xml_from_tar()

    def _parse_filepath(self):
        """Set the year, run, subrun, and part from the file name."""
        self.run = I3FileMetadata.parse_run_number(self.file)

        # Ex: key_31445930_PFRaw_PhysicsFiltering_Run00128000_Subrun00000000_00000156.tar.gz
        # Ex: ukey_b98a353f-72e8-4d2e-afd7-c41fa5c8d326_PFRaw_PhysicsFiltering_Run00131322_Subrun00000000_00000018.tar.gz
        # Ex: ukey_05815dd9-2411-468c-9bd5-e99b8f759efd_PFRaw_RandomFiltering_Run00130470_Subrun00000060_00000000.tar.gz
        # Ex: PFRaw_PhysicsTrig_PhysicsFiltering_Run00114085_Subrun00000000_00000208.tar.gz
        # Ex: PFRaw_TestData_PhysicsFiltering_Run00114672_Subrun00000000_00000011.tar.gz
        # Ex: PFRaw_TestData_RandomFiltering_Run00113816_Subrun00000033_00000000.tar.gz
        if re.match(r'(.*)_Run[0-9]+_Subrun[0-9]+_[0-9]+(.*)', self.file.name):
            self.season_year = None
            s = self.file.name.split('Subrun')[1]
            self.subrun = int(s.split('_')[0])
            p = self.file.name.split('_')[-1]
            self.part = int(p.split('.')[0])
        else:
            raise Exception(f"Filename not in a known PFRaw file format, {self.file.name}.")

    @staticmethod
    def is_file(file, processing_level):
        """ True if PFRaw and the file is in the [...#].tar.[...] file format. """
        # Ex. key_31445930_PFRaw_PhysicsFiltering_Run00128000_Subrun00000000_00000156.tar.gz
        return (processing_level == ProcessingLevel.PFRaw) and I3FileMetadata._is_run_tar_file(file)


class MetadataManager:
    """Commander class for handling metadata for different file types"""

    def __init__(self, site, basic_only=False):
        self.dir_path = ""
        self.site = site
        self.basic_only = basic_only
        self.l2_dir_metadata = {}

    def _prep_l2_dir_metadata(self):
        """Get metadata-related files for later processing with individual i3 files."""
        self.l2_dir_metadata = {}
        dir_meta_xml = None
        gaps_files = {}  # gaps_files[<filename w/o extension>]
        gcd_files = {}  # gcd_files[<run id w/o leading zeros>]
        for dir_entry in os.scandir(self.dir_path):
            if not dir_entry.is_file():
                continue
            # Meta XML (one per directory)
            if "meta.xml" in dir_entry.name:  # Ex. level2_meta.xml, level2pass2_meta.xml
                if dir_meta_xml is not None:
                    raise Exception(f"Multiple *meta.xml files found in {self.dir_path}.")
                try:
                    with open(dir_entry.path, 'r') as xml_file:
                        dir_meta_xml = xmltodict.parse(xml_file.read())
                except xml.parsers.expat.ExpatError:
                    pass
            # Gaps Files (one per i3 file)
            elif "_GapsTxt.tar" in dir_entry.name:  # Ex. Run00130484_GapsTxt.tar
                try:
                    with tarfile.open(dir_entry.path) as tar:
                        for tar_obj in tar:
                            file_dict = yaml.safe_load(tar.extractfile(tar_obj))
                            # Ex. Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188_gaps.txt
                            no_extension = tar_obj.name.split("_gaps.txt")[0]
                            gaps_files[no_extension] = file_dict
                except tarfile.ReadError:
                    pass
            # GCD Files (one per run)
            elif "GCD" in dir_entry.name:  # Ex. Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst
                run = I3FileMetadata.parse_run_number(dir_entry)
                gcd_files[str(run)] = dir_entry.path
        self.l2_dir_metadata['dir_meta_xml'] = dir_meta_xml
        self.l2_dir_metadata['gaps_files'] = gaps_files
        self.l2_dir_metadata['gcd_files'] = gcd_files

    def new_file(self, filepath):
        """Factory method for returning different metadata-file types"""
        file = FileInfo(filepath)
        if not self.basic_only:
            processing_level = ProcessingLevel.from_filename(file.name)
            # L2
            if L2FileMetadata.is_file(file, processing_level):
                # get directory's metadata
                file_dir_path = os.path.dirname(os.path.abspath(file.path))
                if (not self.l2_dir_metadata) or (file_dir_path != self.dir_path):
                    self.dir_path = file_dir_path
                    self._prep_l2_dir_metadata()
                try:
                    no_extension = file.name.split(".i3")[0]
                    gaps = self.l2_dir_metadata['gaps_files'][no_extension]
                except KeyError:
                    gaps = {}
                try:
                    run = I3FileMetadata.parse_run_number(file)
                    gcd = self.l2_dir_metadata['gcd_files'][str(run)]
                except KeyError:
                    gcd = ""
                logging.debug(f'Gathering L2 metadata for {file.name}...')
                return L2FileMetadata(file, self.site, self.l2_dir_metadata['dir_meta_xml'], gaps, gcd)
            # PFFilt
            if PFFiltFileMetadata.is_file(file, processing_level):
                logging.debug(f'Gathering PFFilt metadata for {file.name}...')
                return PFFiltFileMetadata(file, self.site)
            # PFDST
            if PFDSTFileMetadata.is_file(file, processing_level):
                logging.debug(f'Gathering PFDST metadata for {file.name}...')
                return PFDSTFileMetadata(file, self.site)
            # PFRaw
            if PFRawFileMetadata.is_file(file, processing_level):
                logging.debug(f'Gathering PFRaw metadata for {file.name}...')
                return PFRawFileMetadata(file, self.site)
            # if no match, fall-through to BasicFileMetadata...
        # Other/ Basic
        logging.debug(f'Gathering basic metadata for {file.name}...')
        return BasicFileMetadata(file, self.site)


async def request_post_patch(fc_rc, metadata, dont_patch=False):
    """POST metadata, and PATCH if file is already in the file catalog."""
    try:
        _ = await fc_rc.request("POST", '/api/files', metadata)
        logging.debug('POSTed.')
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            if dont_patch:
                logging.debug('File already exists, not replacing.')
            else:
                patch_path = e.response.json()['file']  # /api/files/{uuid}
                _ = await fc_rc.request("PATCH", patch_path, metadata)
                logging.debug('PATCHed.')
        else:
            raise
    return fc_rc


async def process_file(filepath, manager, fc_rc, no_patch):
    """Gather and POST metadata for a file."""
    try:
        metadata_file = manager.new_file(filepath)
        metadata = metadata_file.generate()
    # OSError is thrown for special files like sockets
    except (OSError, PermissionError, FileNotFoundError) as e:
        logging.exception(f'{filepath} not gathered, {e.__class__.__name__}.')
        return
    except:
        logging.exception(f'Unexpected exception raised for {filepath}.')
        raise

    logging.debug(f'{filepath} gathered.')
    logging.debug(metadata)
    await request_post_patch(fc_rc, metadata, no_patch)


async def process_paths(paths, manager, fc_rc, no_patch):
    """POST metadata of files given by paths, and return any directories."""
    sub_files = []

    for p in paths:
        if is_processable_path(p):
            if os.path.isfile(p):
                await process_file(p, manager, fc_rc, no_patch)
            elif os.path.isdir(p):
                logging.debug(f'Directory found, {p}. Queuing its contents...')
                try:
                    sub_files.extend(dir_entry.path for dir_entry in os.scandir(p)
                                     if not dir_entry.is_symlink())  # don't add symbolic links
                except (PermissionError, FileNotFoundError):
                    continue
        else:
            logging.debug(f'Skipping {p}, not a directory nor file.')

    return sub_files


def path_in_blacklist(path, blacklist):
    """Return True if path is in the blacklist."""
    for b in blacklist:
        if path.startswith(b):
            logging.debug(f'Skipping {path}, file and/or directory is in blacklist ({b}).')
            return True
    return False


def process_work(paths, args, blacklist):
    """Wrap async function, process_paths. Return files nested under any directories."""
    if not isinstance(paths, list):
        raise TypeError(f'`paths` object is not list {paths}')
    if not paths:
        return []

    # Check blacklist
    paths = [p for p in paths if not path_in_blacklist(p, blacklist)]

    # Process Paths
    fc_rc = RestClient(args.url, token=args.token, timeout=args.timeout, retries=args.retries)
    manager = MetadataManager(args.site, args.basic_only)
    sub_files = asyncio.get_event_loop().run_until_complete(
        process_paths(paths, manager, fc_rc, args.no_patch))

    fc_rc.close()
    return sub_files


def check_path(path):
    """Check if path is rooted at a white-listed root path."""
    for root in ACCEPTED_ROOTS:
        if path.startswith(root):
            return
    message = f"{path} is not rooted at: {', '.join(ACCEPTED_ROOTS)}"
    logging.critical(message)
    raise Exception(f'Invalid path ({message}).')


def gather_file_info(starting_paths, blacklist, args):
    """Gather and post metadata from files rooted at starting_paths. Do this multi-processed."""
    # Get full paths
    starting_paths = [os.path.abspath(p) for p in starting_paths]
    for p in starting_paths:
        check_path(p)

    # Traverse paths and process files
    futures = []
    with ProcessPoolExecutor() as pool:
        queue = starting_paths
        split = math.ceil(len(queue) / args.processes)
        while futures or queue:
            logging.debug(f'Queue: {len(queue)}.')
            # Divvy up queue among available worker(s). Each worker gets 1/nth of the queue.
            if queue:
                queue.sort()
                while args.processes != len(futures):
                    paths, queue = queue[:split], queue[split:]
                    logging.debug(f'Worker Assigned: {len(futures)+1}/{args.processes} ({len(paths)} paths).')
                    futures.append(pool.submit(process_work, paths, args, blacklist))
            logging.debug(f'Workers: {len(futures)} {futures}.')
            # Extend the queue
            while not futures[0].done():  # concurrent.futures.wait(FIRST_COMPLETED) is slower
                sleep(0.1)
            future = futures.pop(0)
            result = future.result()
            if result:
                queue.extend(result)
                split = math.ceil(len(queue) / args.processes)
            logging.debug(f'Worker finished: {future} (enqueued {len(result)}).')


def sorted_unique(infile, others=None):
    """Read in lines from file, aggregate with those in others list. Return all unique lines, sorted."""
    lines = []
    if others:
        lines.extend(others)
    if infile:
        with open(infile) as file:
            lines.extend(l.rstrip() for l in file)
    lines = [l for l in sorted(set(lines)) if l]
    return lines


def main():
    """Main."""
    parser = argparse.ArgumentParser(description='Find files under PATH(s), compute their metadata and '
                                     'upload it to File Catalog.',
                                     epilog='Notes: (1) symbolic links are never followed.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('paths', metavar='PATHS', nargs='*',
                        help='path(s) to scan for files.')
    parser.add_argument('--paths-file', dest='paths_file', default=None,
                        help='file containing path(s) to scan for files. (use this option for a large number of paths)')
    parser.add_argument('--processes', type=int, default=1,
                        help='number of process for multi-processing')
    parser.add_argument('-u', '--url', default='https://file-catalog.icecube.wisc.edu/',  # 'http://localhost:8888'
                        help='File Catalog URL')
    parser.add_argument('-s', '--site', required=True,
                        help='site value of the "locations" object')
    parser.add_argument('-t', '--token', required=True,
                        help='LDAP token')
    parser.add_argument('--timeout', type=int, default=15,
                        help='REST client timeout duration')
    parser.add_argument('--retries', type=int, default=3,
                        help='REST client number of retries')
    parser.add_argument('--basic-only', dest='basic_only', default=False, action='store_true',
                        help='only collect basic metadata')
    parser.add_argument('--no-patch', dest='no_patch', default=False, action='store_true',
                        help='do not PATCH if the file already exists in the file catalog')
    parser.add_argument('--blacklist-file', dest='blacklist_file',
                        help='blacklist file containing all paths to skip')
    parser.add_argument('-l', '--log', default='DEBUG',
                        help='the output logging level')
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log.upper()))
    for arg, val in vars(args).items():
        logging.info(f'{arg}: {val}')

    logging.info(f'Collecting metadata from {args.paths} and those in file (at {args.paths_file})...')

    # Aggregate and sort all paths
    paths = sorted_unique(args.paths_file, others=args.paths)

    # Read blacklisted paths
    blacklist = sorted_unique(args.blacklist_file)

    gather_file_info(paths, blacklist, args)


if __name__ == '__main__':
    main()
