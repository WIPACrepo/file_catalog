"""Test indexer filename parsing."""

import pytest
from indexer import (I3FileMetadata, L2FileMetadata, PFDSTFileMetadata, PFFiltFileMetadata,
                     PFRawFileMetadata, fix_known_filepath_issues)


def test_run_number():
    """Run generic filename parsing of run number."""
    assert I3FileMetadata.parse_run_number(
        'Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst') == 130484

    assert I3FileMetadata.parse_run_number(
        'Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst') == 130567

    assert I3FileMetadata.parse_run_number('Run00125791_GapsTxt.tar') == 125791

    assert I3FileMetadata.parse_run_number(
        'Level2_IC86.2015_24HrTestRuns_data_Run00126291_Subrun00000203.i3.bz2') == 126291

    # Errors
    errors_filenames = [
        'Level2_IC86.2011_corsika.011690.000796.i3.bz2',
        'SunEvents_Level2_IC79_data_test1.i3.bz2',
        'logfiles_PFDST_2011.tar.gz',
        'DebugData_PFRaw124751_001.tar.gz',
        'logfiles_PFDST_2010.tar.gz'
    ]

    for filename in errors_filenames:
        print(f"FILNAMES: {filename}")
        with pytest.raises(Exception) as e:
            I3FileMetadata.parse_run_number(filename)
        assert "No run number found in filename," in str(e.value)


def _test_filenames_parsing(filenames_and_values, patterns):
    for filename, values in filenames_and_values.items():
        print(filename)
        y, r, s, p = I3FileMetadata.parse_year_run_subrun_part(patterns, filename)
        print(f"OUTPUTS: {y}, {r}, {s}, {p}")
        assert y == values[0]
        assert r == values[1]
        assert s == values[2]
        assert p == values[3]


def _test_bad_filenames_parsing(bad_filenames, patterns):
    for filename in bad_filenames:
        print(filename)

        with pytest.raises(ValueError) as e:
            I3FileMetadata.parse_year_run_subrun_part(patterns, filename)
        assert "Filename does not match any pattern, " in str(e.value)


def _test_valid_filenames(filenames, is_valid_filename_function):
    for f in filenames:
        print(f)
        assert is_valid_filename_function(f)


def _test_bad_valid_filenames_parsing(bad_filenames, is_valid_filename_function):
    for f in bad_filenames:
        print(f)
        assert not is_valid_filename_function(f)


def test_L2():
    """Run L2 filename parsing."""
    filenames_and_values = {
        'Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst': ['2017', 130567, 0, 280],
        'Level2pass2_IC79.2010_data_Run00115975_Subrun00000000_00000055.i3.zst': ['2010', 115975, 0, 55],
        'Level2_PhysicsTrig_PhysicsFiltering_Run00120374_Subrun00000000_00000001.i3': [None, 120374, 0, 1],
        'Level2pass3_PhysicsFiltering_Run00127353_Subrun00000000_00000000.i3.gz': [None, 127353, 0, 0],
        'Level2_IC86.2016_data_Run00129004_Subrun00000316.i3.bz2': ['2016', 129004, 0, 316],
        'Level2_IC86.2012_Test_data_Run00120028_Subrun00000081.i3.bz2': ['2012', 120028, 0, 81],
        'Level2_IC86.2015_24HrTestRuns_data_Run00126291_Subrun00000203.i3.bz2': ['2015', 126291, 0, 203],
        'Level2_IC86.2011_data_Run00119221_Part00000126.i3.bz2': ['2011', 119221, 0, 126],
        'Level2a_IC59_data_Run00115968_Part00000290.i3.gz': ['2009', 115968, 0, 290],
        'MoonEvents_Level2_IC79_data_Run00116082_NewPart00000613.i3.gz': ['2010', 116082, 0, 613],
        'Level2_All_Run00111562_Part00000046.i3.gz': [None, 111562, 0, 46],
        'Level2_IC86.2018RHEL_6_V05-02-00b_py2-v311_data_Run00132765_Subrun00000000_00000000.i3.zst': ['2018', 132765, 0, 0],
        'Level2pass3_PhysicsFiltering_Run00127765_Subrun00000000_00000005.i3.gz': [None, 127765, 0, 5],
        'Level2_PhysicsTrig_PhysicsFiltering_Run00120374_Subrun00000000_00000001_new2.i3': [None, 120374, 0, 1],
        'MoonEvents_Level2_All_Run00111887_part2.i3.gz': [None, 111887, 0, 2]
    }

    _test_valid_filenames(filenames_and_values.keys(), L2FileMetadata.is_valid_filename)
    _test_filenames_parsing(filenames_and_values, L2FileMetadata.FILENAME_PATTERNS)


def test_bad_L2():
    """Run bad L2 filename parsing."""
    filenames = [
        'Level2_IC86.2011_corsika.011690.000796.i3.bz2',
        'Level2_nugen_numu_ic40_twr.001825.000900.i3.gz',
        'SunEvents_Level2_IC79_data_test1.i3.bz2',
        'Level2_IC86.2013_corsika.012016.000008.i3',
        'Level2_nugen_numu_ic40_twr.001825.000000.i3.gz',
        'Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst',
        'level2_meta.xml',
        'Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188_gaps.txt',
        'Level2_Run00088383.i3.gz',
        'Level2_Run0002484.i3',
        'MoonEvents_Level2_All_Run00110864.i3.gz',
        'Level2_All_Run00111399-000.i3.gz',
        'IC59_MoonEvents_Level2_IC59_data_Run00114554.i3.gz'
    ]

    _test_bad_valid_filenames_parsing(filenames, L2FileMetadata.is_valid_filename)
    _test_bad_filenames_parsing(filenames, L2FileMetadata.FILENAME_PATTERNS)


def test_PFFilt():
    """Run PFFilt filename parsing."""
    filenames_and_values = {
        'PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2': [None, 131989, 0, 295],
        'PFFilt_PhysicsTrig_PhysicsFiltering_Run00121503_Subrun00000000_00000314.tar.bz2': [None, 121503, 0, 314],
        'orig.PFFilt_PhysicsFiltering_Run00127080_Subrun00000000_00000244.tar.bz2.orig': [None, 127080, 0, 244],
        'PFFilt_PhysicsTrig_PhysicsFilt_Run00089959_00180.tar.gz': [None, 89959, 0, 180],
        'PFFilt_PhysicsTrig_RandomFilt_Run86885_006.tar.gz': [None, 86885, 0, 6]
    }

    _test_valid_filenames(filenames_and_values.keys(), PFFiltFileMetadata.is_valid_filename)
    _test_filenames_parsing(filenames_and_values, PFFiltFileMetadata.FILENAME_PATTERNS)


def test_PFDST():
    """Run PFDST filename parsing."""
    filenames_and_values = {
        'ukey_fa818e64-f6d2-4cc1-9b34-e50bfd036bf3_PFDST_PhysicsFiltering_Run00131437_Subrun00000000_00000066.tar.gz': [None, 131437, 0, 66],
        'ukey_42c89a63-e3f7-4c3e-94ae-840eff8bd4fd_PFDST_RandomFiltering_Run00131155_Subrun00000051_00000000.tar.gz': [None, 131155, 51, 0],
        'PFDST_PhysicsFiltering_Run00125790_Subrun00000000_00000064.tar.gz': [None, 125790, 0, 64],
        'PFDST_UW_PhysicsFiltering_Run00125832_Subrun00000000_00000000.tar.gz': [None, 125832, 0, 0],
        'PFDST_RandomFiltering_Run00123917_Subrun00000000_00000000.tar.gz': [None, 123917, 0, 0],
        'PFDST_PhysicsTrig_PhysicsFiltering_Run00121663_Subrun00000000_00000091.tar.gz': [None, 121663, 0, 91],
        'PFDST_TestData_PhysicsFiltering_Run00122158_Subrun00000000_00000014.tar.gz': [None, 122158, 0, 14],
        'PFDST_TestData_RandomFiltering_Run00119375_Subrun00000136_00000000.tar.gz': [None, 119375, 136, 0],
        'PFDST_TestData_Unfiltered_Run00119982_Subrun00000000_000009.tar.gz': [None, 119982, 0, 9]
    }

    _test_valid_filenames(filenames_and_values.keys(), PFDSTFileMetadata.is_valid_filename)
    _test_filenames_parsing(filenames_and_values, PFDSTFileMetadata.FILENAME_PATTERNS)


def test_bad_PFDST():
    """Run bad PFDST filename parsing."""
    filenames = [
        'logfiles_PFDST_2011.tar.gz',
        'logfiles_PFDST_2010.tar.gz'
    ]

    _test_bad_valid_filenames_parsing(filenames, PFDSTFileMetadata.is_valid_filename)
    _test_bad_filenames_parsing(filenames, PFDSTFileMetadata.FILENAME_PATTERNS)


def test_PFRaw():
    """Run PFRaw filename parsing."""
    filenames_and_values = {
        'key_31445930_PFRaw_PhysicsFiltering_Run00128000_Subrun00000000_00000156.tar.gz': [None, 128000, 0, 156],
        'ukey_b98a353f-72e8-4d2e-afd7-c41fa5c8d326_PFRaw_PhysicsFiltering_Run00131322_Subrun00000000_00000018.tar.gz': [None, 131322, 0, 18],
        'ukey_05815dd9-2411-468c-9bd5-e99b8f759efd_PFRaw_RandomFiltering_Run00130470_Subrun00000060_00000000.tar.gz': [None, 130470, 60, 0],
        'PFRaw_PhysicsTrig_PhysicsFiltering_Run00114085_Subrun00000000_00000208.tar.gz': [None, 114085, 0, 208],
        'PFRaw_TestData_PhysicsFiltering_Run00114672_Subrun00000000_00000011.tar.gz': [None, 114672, 0, 11],
        'PFRaw_TestData_RandomFiltering_Run00113816_Subrun00000033_00000000.tar.gz': [None, 113816, 33, 0],
        'EvtMonPFRaw_PhysicsTrig_RandomFiltering_Run00106489_Subrun00000000.tar.gz': [None, 106489, 0, 0],
        'DebugData_PFRaw_Run110394_1.tar.gz': [None, 110394, 0, 1],
        'DebugData-PFRaw_RF_Run00129213_Subrun00000001.tar.gz.tar.gz': [None, 129213, 0, 1],
        'DebugData-PFRaw_RF_Run00129335_SR01_00.tar.gz.tar.gz': [None, 129335, 0, 1]
    }

    _test_valid_filenames(filenames_and_values.keys(), PFRawFileMetadata.is_valid_filename)
    _test_filenames_parsing(filenames_and_values, PFRawFileMetadata.FILENAME_PATTERNS)


def test_bad_PFRaw():
    """Run bad PFRaw filename parsing."""
    filenames = [
        'DebugData_PFRaw124751_001.tar.gz',
        'DebugData_PFRaw_Run_115244_v5.tar.gz',
        'DebugData_PFRaw_TestData_PhysicsFiltering_Run00111448.tar.gz',
        'DebugData-PFRaw_TestData_Run00118957.tar.gz',
        'DebugData-PFRaw_PhysicsTrig_PhysicsFiltering_Run00119158.tar.gz',
        'EvtMonPFRaw_PhysicsTrig_RandomFilt_Run86510.tar.gz',
        'EvtMonPFRaw_PhysicsTrig_PhysicsFilt_Run00089012.tar.gz'
    ]

    _test_bad_valid_filenames_parsing(filenames, PFRawFileMetadata.is_valid_filename)
    _test_bad_filenames_parsing(filenames, PFRawFileMetadata.FILENAME_PATTERNS)


def test_bad_patterns():
    """Run PFRaw filename parsing."""
    bad_patterns = [
        r'.*\.(?P<year>20\d{2})_Subrun(?P<subrun>\d+)',
        r'.*\.(?P<run>20\d{2})_Subrun(?P<subrun>\d+)',
        r'.*\.(?P<year>20\d{2})_Subrun(?P<part>\d+)'
    ]

    for bp in bad_patterns:
        with pytest.raises(Exception) as e:
            I3FileMetadata.parse_year_run_subrun_part([bp], 'filename-wont-be-matched-anyways')
        assert "Pattern does not have `run` and `part` regex groups," in str(e.value)


def test_hard_coded_filepath_fixes():
    """Run hard_coded_filepath_fixes() tests."""
    raw_lines = [
        (
            '/data/exp/IceCube/2011/unbiased/AURA_Processed/0824/MDAQ-Run-186971/headers_unsorted.txt.new',
            None
        ),
        (
            '/data/exp/IceCube/2011/unbiased/AURA_Processed/0824/MDAQ-Run-186971/headers_unsorted.txt/data/exp/IceCube/2011/unbiased/AURA_Processed/0706/MDAQ-Run-175122/mdaq.hdr',
            [
                '/data/exp/IceCube/2011/unbiased/AURA_Processed/0824/MDAQ-Run-186971/headers_unsorted.txt',
                '/data/exp/IceCube/2011/unbiased/AURA_Processed/0706/MDAQ-Run-175122/mdaq.hdr'
            ],
        ),
        (
            '/data/exp/IceCube/2011/unbiased/AURA_Processed/0824/MDAQ-Run-186971/mdaq.hdr',
            None
        ),
        (
            '/data/exp/IceCube/2012/filtered/level2/0815/Level2_IC86.2012_data_Run00120559_Subrun00000015_DST.root',
            None
        ),
        (
            '/data/exp/IceCube/2012/filtered/level2/0815/Level2_IC86.2012_data_Run00120559_Subrun00000015_IT.i3.bz2/data/exp/IceCube/2008/unbiased/acoustic/0112/spats_strBpts_200801120704.tar.gz',
            [
                '/data/exp/IceCube/2012/filtered/level2/0815/Level2_IC86.2012_data_Run00120559_Subrun00000015_IT.i3.bz2',
                '/data/exp/IceCube/2008/unbiased/acoustic/0112/spats_strBpts_200801120704.tar.gz'
            ]
        ),
        (
            '/data/exp/IceCube/2012/filtered/level2/0815/Level2_IC86.2012_data_Run00120559_Subrun00000016.i3.bz2',
            None
        )
    ]

    for raw, fixed in raw_lines:
        print(f"{raw} -- {fixed}")
        assert fixed == fix_known_filepath_issues(raw)
